#include <Common/RegexpJIT/RegexpProgram.h>

#include <limits>


namespace DB::RegexpJIT
{

namespace
{

constexpr uint32_t UNBOUNDED = std::numeric_limits<uint32_t>::max();

/// Maximum number of `Optional` constructs we are willing to compile. Each one can duplicate the
/// continuation in the generated code, so without a cap the code size could grow exponentially.
constexpr int MAX_OPTIONALS = 4;
/// We only JIT-compile patterns that match greedily without any backtracking, so matching is always
/// linear in the input (never slower than the general engine). A quantifier needs backtracking when
/// its stop is ambiguous (the following first-byte set overlaps the quantified set and it is not
/// right-anchored) - e.g. `.*a`; such patterns fall back to the general engine. A quantifier whose
/// stop is unambiguous - e.g. `.+`, `[^/]+/`, `.*$` - is matched in a single greedy pass.
/// See `analyzeFirst`.
constexpr int MAX_NONDET_QUANTIFIERS = 0;
/// Guard against pathological nesting depth.
constexpr int MAX_DEPTH = 16;

bool isAscii(uint8_t c) { return c < 0x80; }

/// Recursive-descent parser for the supported subset. Bails out (sets `failed`) for anything else.
class Parser
{
public:
    Parser(std::string_view pattern_, const ParseFlags & flags_)
        : pattern(pattern_)
        , case_insensitive(flags_.case_insensitive)
        , dot_all(flags_.dot_all)
    {
    }

    std::optional<RegexpProgram> parse()
    {
        RegexpProgram program;

        consumeLeadingFlags();

        if (peek() == '^')
        {
            program.anchored_start = true;
            ++pos;
        }

        program.ops = parseSequence(/* depth */ 0, /* in_group */ false);

        /// A trailing `$` (only valid at the very end of the top-level sequence) is recorded as
        /// `anchored_end`; `parseSequence` leaves it for us to consume here.
        if (!failed && peek() == '$' && pos + 1 == pattern.size())
        {
            program.anchored_end = true;
            ++pos;
        }

        if (failed || pos != pattern.size())
            return std::nullopt;
        if (optionals_count > MAX_OPTIONALS)
            return std::nullopt;

        program.num_captures = next_capture_index;
        program.case_insensitive = case_insensitive;
        program.dot_all = dot_all;
        return program;
    }

private:
    std::string_view pattern;
    size_t pos = 0;
    int next_capture_index = 1; /// group 0 is the whole match, reserved.
    int optionals_count = 0;
    bool case_insensitive = false;
    bool dot_all = false;
    bool failed = false;

    char peek(size_t ahead = 0) const { return pos + ahead < pattern.size() ? pattern[pos + ahead] : '\0'; }
    void bail() { failed = true; }

    /// Recognize a leading global flag group like `(?i)`, `(?s)`, `(?is)`.
    void consumeLeadingFlags()
    {
        if (!(peek() == '(' && peek(1) == '?'))
            return;
        size_t p = pos + 2;
        bool saw_flag = false;
        bool ci = case_insensitive;
        bool da = dot_all;
        while (p < pattern.size() && pattern[p] != ')' && pattern[p] != ':')
        {
            switch (pattern[p])
            {
                case 'i': ci = true; break;
                case 's': da = true; break;
                case 'm': return; /// multiline not supported - leave the group for the main parser to reject.
                case 'U': return; /// ungreedy flag flips greediness globally - not supported.
                case '-': return; /// negated flags - not supported.
                default: return;
            }
            saw_flag = true;
            ++p;
        }
        if (saw_flag && p < pattern.size() && pattern[p] == ')')
        {
            case_insensitive = ci;
            dot_all = da;
            pos = p + 1;
        }
    }

    /// Parse a sequence of atoms until end of input, or until a `)` when inside a group.
    std::vector<Op> parseSequence(int depth, bool in_group)
    {
        std::vector<Op> ops;
        std::vector<uint8_t> literal_run;

        auto flush_literal = [&]()
        {
            if (literal_run.empty())
                return;
            Op op;
            op.kind = OpKind::Literal;
            op.literal = std::move(literal_run);
            literal_run.clear();
            ops.push_back(std::move(op));
        };

        if (depth > MAX_DEPTH)
        {
            bail();
            return ops;
        }

        while (!failed && pos < pattern.size())
        {
            char c = pattern[pos];

            if (in_group && c == ')')
                break;

            switch (c)
            {
                case ')':
                    bail(); /// Unbalanced close paren at top level.
                    break;

                case '|':
                    bail(); /// Alternation is not supported yet.
                    break;

                case '^':
                    bail(); /// Interior `^` (leading one already consumed by `parse`).
                    break;

                case '$':
                    /// Only a trailing top-level `$` is allowed; `parse` will consume it.
                    if (!in_group && pos + 1 == pattern.size())
                        return flush_literal(), ops;
                    bail();
                    break;

                case '.':
                {
                    flush_literal();
                    ++pos; /// consume '.'
                    CharSet set;
                    set.add('\n');
                    set.invert(); /// `.` = everything except newline ...
                    if (dot_all)
                        set.add('\n'); /// ... unless DOT_NL is set.
                    Op op;
                    op.kind = OpKind::CharQuant;
                    op.set = set;
                    if (!applyQuantifier(op))
                        bail();
                    else
                        ops.push_back(std::move(op));
                    break;
                }

                case '[':
                {
                    flush_literal();
                    Op op;
                    if (!parseCharClass(op))
                        bail();
                    else
                        ops.push_back(std::move(op));
                    break;
                }

                case '(':
                {
                    flush_literal();
                    if (!parseGroup(depth, ops))
                        bail();
                    break;
                }

                case '\\':
                {
                    /// An escape is either a literal byte or a shorthand class.
                    CharSet shorthand;
                    bool is_class = false;
                    uint8_t literal_byte = 0;
                    if (!parseEscape(shorthand, is_class, literal_byte))
                    {
                        bail();
                        break;
                    }
                    if (is_class)
                    {
                        flush_literal();
                        Op op;
                        op.kind = OpKind::CharQuant;
                        op.set = shorthand;
                        if (!applyQuantifier(op))
                            bail();
                        else
                            ops.push_back(std::move(op));
                    }
                    else
                    {
                        handleSingleChar(literal_byte, literal_run, ops, flush_literal);
                    }
                    break;
                }

                case '*':
                case '+':
                case '?':
                case '{':
                    bail(); /// Quantifier without a preceding atom.
                    break;

                default:
                {
                    if (!isAscii(static_cast<uint8_t>(c)) && case_insensitive)
                    {
                        /// Case-insensitive matching of non-ASCII bytes would require Unicode folding.
                        bail();
                        break;
                    }
                    ++pos; /// consume the character.
                    handleSingleChar(static_cast<uint8_t>(c), literal_run, ops, flush_literal);
                    break;
                }
            }
        }

        flush_literal();
        return ops;
    }

    /// A single byte that may carry a quantifier. Without a quantifier it joins the literal run;
    /// with one it becomes a (positive, single-member) `CharQuant`.
    /// Precondition: the character byte has already been consumed (pos points just past it).
    template <typename FlushLiteral>
    void handleSingleChar(uint8_t byte, std::vector<uint8_t> & literal_run, std::vector<Op> & ops, FlushLiteral && flush_literal)
    {
        char q = peek();
        if (q == '*' || q == '+' || q == '?' || q == '{')
        {
            if (!isAscii(byte))
            {
                /// Quantifying a single non-ASCII byte would split a multi-byte code point.
                bail();
                return;
            }
            flush_literal();
            Op op;
            op.kind = OpKind::CharQuant;
            op.set.add(byte);
            if (case_insensitive)
                op.set.foldAsciiCase();
            if (!applyQuantifier(op)) /// single ASCII char: any quantifier is safe.
                bail();
            else
                ops.push_back(std::move(op));
        }
        else
        {
            literal_run.push_back(byte);
        }
    }

    /// Apply the quantifier (if any) that follows a `CharQuant` op whose `set` is already filled.
    /// Fixed counts (`?`, `{n,m}`, or an implicit count of 1) are span-safe only for an ASCII-only set,
    /// where every member is a single byte. A set that contains any byte >= 0x80 (`.`, a negated class,
    /// `\D`/`\W`/`\S`, or any class that happens to include a non-ASCII byte) can match a UTF-8
    /// continuation byte or a fragment of a multi-byte code point, so only the maximal runs `*` and `+`
    /// stay equivalent to RE2's code-point matching (UTF-8 is self-synchronizing).
    bool applyQuantifier(Op & op)
    {
        const bool allow_fixed = op.set.isAsciiOnly();
        char q = peek();
        if (q == '*')
        {
            ++pos;
            op.min = 0;
            op.max = UNBOUNDED;
        }
        else if (q == '+')
        {
            ++pos;
            op.min = 1;
            op.max = UNBOUNDED;
        }
        else if (q == '?')
        {
            if (!allow_fixed)
                return false;
            ++pos;
            op.min = 0;
            op.max = 1;
        }
        else if (q == '{')
        {
            if (!allow_fixed)
                return false;
            if (!parseBraceQuantifier(op))
                return false;
        }
        else
        {
            if (!allow_fixed)
                return false; /// A bare `.` or `[^...]` (fixed count 1) is not span-safe.
            op.min = 1;
            op.max = 1;
        }

        /// Non-greedy variants (`*?`, `+?`, ...) are not supported.
        if (peek() == '?')
            return false;
        /// Possessive variants (`*+`, ...) are not supported.
        if (peek() == '+')
            return false;
        return true;
    }

    bool parseBraceQuantifier(Op & op)
    {
        /// pos points at '{'. Parse {n}, {n,}, {n,m}.
        size_t p = pos + 1;
        auto read_number = [&](uint32_t & out) -> bool
        {
            size_t start = p;
            uint64_t value = 0;
            while (p < pattern.size() && pattern[p] >= '0' && pattern[p] <= '9')
            {
                value = value * 10 + static_cast<uint64_t>(pattern[p] - '0');
                if (value > 1000000)
                    return false;
                ++p;
            }
            if (p == start)
                return false;
            out = static_cast<uint32_t>(value);
            return true;
        };

        uint32_t lo = 0;
        if (!read_number(lo))
            return false;
        uint32_t hi = lo;
        if (p < pattern.size() && pattern[p] == ',')
        {
            ++p;
            if (p < pattern.size() && pattern[p] == '}')
                hi = UNBOUNDED;
            else if (!read_number(hi))
                return false;
        }
        if (p >= pattern.size() || pattern[p] != '}')
            return false;
        if (hi != UNBOUNDED && hi < lo)
            return false;
        /// RE2 rejects counted repetitions whose bound exceeds `kMaxRepeat` (1000) with a compile-time
        /// error; accepting them here would turn that error into a successful match. Fall back instead.
        constexpr uint32_t MAX_REPEAT = 1000;
        if (lo > MAX_REPEAT || (hi != UNBOUNDED && hi > MAX_REPEAT))
            return false;
        op.min = lo;
        op.max = hi;
        pos = p + 1;
        return true;
    }

    bool parseGroup(int depth, std::vector<Op> & ops)
    {
        /// pos points at '('.
        bool capturing = true;
        size_t body_start = pos + 1;

        if (peek(1) == '?')
        {
            if (peek(2) == ':')
            {
                capturing = false;
                body_start = pos + 3;
            }
            else
            {
                return false; /// Named groups, lookaround, flags-in-group, etc. are not supported.
            }
        }

        int capture_index = -1;
        if (capturing)
            capture_index = next_capture_index++;

        pos = body_start;
        std::vector<Op> body = parseSequence(depth + 1, /* in_group */ true);
        if (failed)
            return false;
        if (peek() != ')')
            return false;
        ++pos; /// consume ')'.

        /// Optional quantifier on the group: only `?` is supported (turns into `Optional`).
        char q = peek();
        bool is_optional = false;
        if (q == '?')
        {
            ++pos;
            if (peek() == '?' || peek() == '+')
                return false; /// non-greedy / possessive.
            is_optional = true;
        }
        else if (q == '*' || q == '+' || q == '{')
        {
            return false; /// Repetition of a group is not supported yet.
        }

        std::vector<Op> group_ops;
        if (capturing)
        {
            Op start_op;
            start_op.kind = OpKind::CaptureStart;
            start_op.capture_index = capture_index;
            group_ops.push_back(std::move(start_op));
        }
        for (auto & op : body)
            group_ops.push_back(std::move(op));
        if (capturing)
        {
            Op end_op;
            end_op.kind = OpKind::CaptureEnd;
            end_op.capture_index = capture_index;
            group_ops.push_back(std::move(end_op));
        }

        if (is_optional)
        {
            ++optionals_count;
            Op op;
            op.kind = OpKind::Optional;
            op.body = std::move(group_ops);
            ops.push_back(std::move(op));
        }
        else
        {
            for (auto & op : group_ops)
                ops.push_back(std::move(op));
        }
        return true;
    }

    bool parseCharClass(Op & op)
    {
        /// pos points at '['.
        ++pos;
        bool negated = false;
        if (peek() == '^')
        {
            negated = true;
            ++pos;
        }

        CharSet set;
        bool first = true;
        while (pos < pattern.size())
        {
            char c = pattern[pos];
            if (c == ']' && !first)
                break;
            first = false;

            /// POSIX named classes like `[[:alpha:]]` / `[[:^digit:]]` are outside the subset and RE2
            /// parses them as a single class, not as the literal bytes `[`, `:`, ... - fall back.
            if (c == '[' && peek(1) == ':')
                return false;

            uint8_t lo_byte = 0;
            if (c == '\\')
            {
                CharSet shorthand;
                bool is_class = false;
                uint8_t literal_byte = 0;
                if (!parseEscape(shorthand, is_class, literal_byte))
                    return false;
                if (is_class)
                {
                    for (unsigned b = 0; b < 256; ++b)
                        if (shorthand.contains(static_cast<uint8_t>(b)))
                            set.add(static_cast<uint8_t>(b));
                    continue;
                }
                lo_byte = literal_byte;
            }
            else
            {
                lo_byte = static_cast<uint8_t>(c);
                ++pos;
            }

            if (!isAscii(lo_byte))
                return false; /// Char classes must be ASCII-only to stay byte/code-point equivalent.

            /// Range like `a-z` (but a trailing `-` before `]` is a literal `-`). Use an explicit
            /// bounds check rather than `peek(1) != '\0'`: a real NUL byte after `-` (e.g. `[a-\0]`,
            /// which arrives as a length-delimited byte) is a genuine range endpoint, so it must reach
            /// the `hi_byte < lo_byte` check below and be rejected exactly like RE2 rejects the
            /// descending range - not be mistaken for the end of the pattern.
            if (peek() == '-' && peek(1) != ']' && pos + 1 < pattern.size())
            {
                ++pos; /// consume '-'.
                uint8_t hi_byte = 0;
                if (peek() == '\\')
                {
                    CharSet shorthand;
                    bool is_class = false;
                    uint8_t literal_byte = 0;
                    if (!parseEscape(shorthand, is_class, literal_byte) || is_class)
                        return false;
                    hi_byte = literal_byte;
                }
                else
                {
                    hi_byte = static_cast<uint8_t>(peek());
                    ++pos;
                }
                if (!isAscii(hi_byte) || hi_byte < lo_byte)
                    return false;
                set.addRange(lo_byte, hi_byte);
            }
            else
            {
                set.add(lo_byte);
            }
        }

        if (pos >= pattern.size() || pattern[pos] != ']')
            return false;
        ++pos; /// consume ']'.

        if (case_insensitive)
            set.foldAsciiCase();
        if (negated)
            set.invert();

        op.kind = OpKind::CharQuant;
        op.set = set;

        /// `applyQuantifier` decides which quantifiers are span-safe from the set itself: a negated class
        /// (or a class that includes a non-ASCII byte, e.g. via `\D`) can match a multi-byte code point,
        /// so only `*`/`+` are allowed; a purely ASCII class is safe with any quantifier.
        return applyQuantifier(op);
    }

    /// Parse the escape sequence after a backslash (pos points at '\\'). Returns false to bail.
    /// Either fills a shorthand class (`is_class = true`) or a single literal byte.
    bool parseEscape(CharSet & shorthand, bool & is_class, uint8_t & literal_byte)
    {
        ++pos; /// consume '\\'.
        if (pos >= pattern.size())
            return false;
        char e = pattern[pos];
        is_class = false;

        auto add_digits = [&](CharSet & s) { s.addRange('0', '9'); };
        auto add_word = [&](CharSet & s) { s.addRange('0', '9'); s.addRange('a', 'z'); s.addRange('A', 'Z'); s.add('_'); };
        /// RE2's Perl `\s` is `[\t\n\f\r ]` and deliberately excludes the vertical tab `\v`.
        auto add_space = [&](CharSet & s) { s.add(' '); s.add('\t'); s.add('\n'); s.add('\r'); s.add('\f'); };

        switch (e)
        {
            case 'd': add_digits(shorthand); is_class = true; ++pos; return true;
            case 'w': add_word(shorthand); is_class = true; ++pos; return true;
            case 's': add_space(shorthand); is_class = true; ++pos; return true;
            case 'D': add_digits(shorthand); shorthand.invert(); is_class = true; ++pos; return true;
            case 'W': add_word(shorthand); shorthand.invert(); is_class = true; ++pos; return true;
            case 'S': add_space(shorthand); shorthand.invert(); is_class = true; ++pos; return true;

            case 'n': literal_byte = '\n'; ++pos; return true;
            case 'r': literal_byte = '\r'; ++pos; return true;
            case 't': literal_byte = '\t'; ++pos; return true;
            case 'f': literal_byte = '\f'; ++pos; return true;
            case 'v': literal_byte = '\v'; ++pos; return true;

            case '0':
                /// RE2 parses an octal escape of up to three digits (`\012` is one byte, newline). We only
                /// handle a bare `\0` (NUL); bail when more octal digits follow so we don't diverge.
                ++pos;
                if (peek() >= '0' && peek() <= '7')
                    return false;
                literal_byte = '\0';
                return true;

            /// Escaped metacharacters and punctuation -> the literal byte.
            case '.': case '\\': case '/': case '(': case ')': case '[': case ']':
            case '{': case '}': case '?': case '*': case '+': case '|': case '^':
            case '$': case '-':
                literal_byte = static_cast<uint8_t>(e);
                ++pos;
                return true;

            default:
                /// `\b`, `\xHH`, Unicode classes, back-references, etc.: bail out conservatively.
                return false;
        }
    }
};

}

/// Compute, for each position, the set of bytes that the continuation can begin with, and use it to
/// mark every `CharQuant` as `deterministic` (its stop is unambiguous: the following first-byte set
/// is disjoint from the quantified set, or it is right-anchored - the following set is empty).
/// A deterministic quantifier matches in a single greedy pass with no backtracking.
/// Side effects: counts variable quantifiers that are NOT deterministic (`nondet_quant`, each of
/// which needs an O(n) give-back) and `Optional` groups (`fork_count`, a bounded 2-way choice).
/// Returns the first-byte set of `ops`, given that what follows them can begin with `follow`.
static CharSet analyzeFirst(std::vector<Op> & ops, const CharSet & follow, int & nondet_quant, int & fork_count)
{
    const size_t n = ops.size();
    std::vector<CharSet> rest_first(n + 1);
    rest_first[n] = follow;

    for (size_t i = n; i-- > 0;)
    {
        Op & op = ops[i];
        const CharSet & after = rest_first[i + 1];
        switch (op.kind)
        {
            case OpKind::Literal:
            {
                CharSet first;
                if (!op.literal.empty())
                    first.add(op.literal[0]);
                rest_first[i] = first; /// a literal is not nullable
                break;
            }
            case OpKind::CharQuant:
            {
                op.deterministic = !op.set.intersects(after);
                if (!op.deterministic && op.min < op.max)
                    ++nondet_quant;
                rest_first[i] = op.set;
                if (op.min == 0) /// can match zero characters, so the continuation is also possible here
                    rest_first[i].unite(after);
                break;
            }
            case OpKind::SuffixAnchor:
            case OpKind::PrefixAnchor:
            case OpKind::CaptureStart:
            case OpKind::CaptureEnd:
                rest_first[i] = after; /// zero-width
                break;
            case OpKind::Optional:
            {
                ++fork_count;
                rest_first[i] = analyzeFirst(op.body, after, nondet_quant, fork_count);
                rest_first[i].unite(after); /// the body may be skipped
                break;
            }
        }
    }

    return rest_first[0];
}

/// RE2 runs case-insensitive matching in UTF-8 mode with Unicode-aware (simple) case folding. The only
/// ASCII letters whose fold class reaches outside ASCII are `k`/`K` (which also fold to U+212A KELVIN
/// SIGN, UTF-8 `E2 84 AA`) and `s`/`S` (which also fold to U+017F LATIN SMALL LETTER LONG S, UTF-8
/// `C5 BF`). Our matcher folds ASCII only and scans byte-wise, so it can match (or, for a maximal run,
/// fail to stop at) those code points differently from RE2. Detect that and fall back to the general
/// engine.
static bool caseInsensitiveTouchesNonAsciiFold(const std::vector<Op> & ops)
{
    for (const Op & op : ops)
    {
        switch (op.kind)
        {
            case OpKind::Literal:
                /// A literal byte `k`/`s` matches only `k`/`K` (`s`/`S`) byte-wise, never the multi-byte
                /// fold code point, while RE2 would also match it.
                for (uint8_t c : op.literal)
                    if (c == 'k' || c == 'K' || c == 's' || c == 'S')
                        return true;
                break;
            case OpKind::CharQuant:
            {
                /// A byte-wise scan agrees with RE2 on a fold code point only when the set's membership of
                /// the ASCII letter equals its membership of every byte of that code point's UTF-8 encoding.
                /// Checking only `set.contains('k'/'s')` (as a previous version did) misses negated classes:
                /// e.g. `(?i)[^a-z]` excludes `k`/`s` after folding and inversion, yet its inverted bitmap
                /// contains the high bytes of KELVIN SIGN / LONG S, so the matcher would accept those bytes
                /// while RE2 (folding them to `k`/`s`) rejects them.
                const CharSet & set = op.set;
                const bool kelvin_matches_as_k = set.contains(0xE2) && set.contains(0x84) && set.contains(0xAA);
                const bool long_s_matches_as_s = set.contains(0xC5) && set.contains(0xBF);
                if (set.contains('k') != kelvin_matches_as_k || set.contains('s') != long_s_matches_as_s)
                    return true;
                break;
            }
            case OpKind::Optional:
                if (caseInsensitiveTouchesNonAsciiFold(op.body))
                    return true;
                break;
            default:
                break;
        }
    }
    return false;
}

/// A proxy for the size of the code the emitter will generate: `emitLiteral` emits one comparison per
/// literal byte, and every other op emits a bounded number of basic blocks (`{n,m}` is not unrolled).
/// Used to keep LLVM compilation cost bounded - see `MAX_PROGRAM_SIZE` in `tryCompileToProgram`.
static size_t programSize(const std::vector<Op> & ops)
{
    size_t size = 0;
    for (const Op & op : ops)
    {
        if (op.kind == OpKind::Literal)
            size += op.literal.size();
        else if (op.kind == OpKind::Optional)
            size += 1 + programSize(op.body);
        else
            size += 1;
    }
    return size;
}

std::optional<RegexpProgram> tryCompileToProgram(std::string_view pattern, const ParseFlags & flags)
{
    if (pattern.empty())
        return std::nullopt;

    /// A case-insensitive pattern containing non-ASCII bytes would need Unicode case folding.
    if (flags.case_insensitive)
        for (unsigned char c : pattern)
            if (!isAscii(c))
                return std::nullopt;

    Parser parser(pattern, flags);
    auto program = parser.parse();
    if (!program)
        return std::nullopt;

    /// `OptimizedRegularExpression` rejects patterns with more than `MAX_SUBPATTERNS` (1024) capturing
    /// groups; accepting them here would turn that error into a successful match. `num_captures` counts
    /// group 0 (the whole match), so the number of capturing groups is `num_captures - 1`.
    constexpr int MAX_SUBPATTERNS = 1024;
    if (program->num_captures - 1 > MAX_SUBPATTERNS)
        return std::nullopt;

    /// See `caseInsensitiveTouchesNonAsciiFold`: a case-insensitive `k`/`s` folds across the ASCII
    /// boundary in RE2's UTF-8 mode, which our ASCII-only folding cannot reproduce.
    if (program->case_insensitive && caseInsensitiveTouchesNonAsciiFold(program->ops))
        return std::nullopt;

    /// A pure unanchored literal (e.g. `abc`, `\.com`) is already handled optimally by the
    /// substring searcher inside `OptimizedRegularExpression`; the JIT's position-by-position scan
    /// would be slower, so defer such patterns to the general engine.
    if (!program->anchored_start && !program->anchored_end && program->ops.size() == 1
        && program->ops[0].kind == OpKind::Literal)
        return std::nullopt;

    /// Bound the generated code size: `emitLiteral` emits one comparison per literal byte, so a pattern
    /// such as `^` + a very large literal + `$` would otherwise make LLVM compile an enormous function
    /// once the compile-count threshold is reached. Such patterns gain nothing from the JIT (RE2 already
    /// searches long literals optimally), so fall back to the general engine.
    constexpr size_t MAX_PROGRAM_SIZE = 256;
    if (programSize(program->ops) > MAX_PROGRAM_SIZE)
        return std::nullopt;

    /// Mark deterministic quantifiers and bound backtracking to keep matching linear (avoid ReDoS):
    /// at most one non-deterministic quantifier (a single O(n) give-back) and a small number of
    /// optional groups (a constant 2^k factor). Anything heavier falls back to the general engine.
    int nondet_quant = 0;
    int fork_count = 0;
    analyzeFirst(program->ops, /* follow */ CharSet{}, nondet_quant, fork_count);
    if (nondet_quant > MAX_NONDET_QUANTIFIERS || fork_count > MAX_OPTIONALS)
        return std::nullopt;

    return program;
}

}
