#pragma once

#include <base/types.h>

#include <memory>
#include <string_view>
#include <vector>


namespace DB
{

/** The regular expressions a Hugging Face `tokenizer.json` writes its pre-tokenizer in.
  *
  * They need lookahead (`\s+(?!\S)`), which RE2 does not support, so they are matched here, for the
  * part of the syntax those files use:
  *
  *   - alternation, `(...)`, `(?:...)`, `(?i:...)`, `(?=...)` and `(?!...)`;
  *   - `?`, `*`, `+` and `{n}`, `{n,}`, `{n,m}`, each greedy, lazy with a trailing `?`, or possessive
  *     with a trailing `+`;
  *   - `.`, `^` and `$` (the start and the end of the text), literal characters and escapes;
  *   - `\s`, `\d`, `\w` and their negations, `\p{...}` and `\P{...}` for a Unicode general category;
  *   - character classes with ranges, negation and the escapes above.
  *
  * Anything else is rejected when the pattern is compiled rather than matched differently.
  *
  * Matching follows Oniguruma, which the `tokenizers` library runs these patterns with: it backtracks,
  * the first alternative that leads to a match wins, and the classes are Unicode ones (`\s` is the
  * `White_Space` property, `\d` is `\p{Nd}`). A byte that does not start a valid UTF-8 sequence is a
  * character of its own that is in no class, so it is matched only by a negated one and by `.`.
  */
class BPEPattern
{
public:
    explicit BPEPattern(std::string_view pattern);
    ~BPEPattern();

    BPEPattern(const BPEPattern &) = delete;
    BPEPattern & operator=(const BPEPattern &) = delete;

    /// The leftmost match in `text` that starts at `from` or later. A match may be empty.
    bool find(std::string_view text, size_t from, size_t & match_begin, size_t & match_end) const;

    struct Node;
    struct CharacterSet;

private:
    struct Continuation;

    bool matchNode(const Node & node, std::string_view text, size_t pos, const Continuation * next, size_t & end) const;
    bool matchNext(std::string_view text, size_t pos, const Continuation * next, size_t & end) const;
    bool matchRepeat(
        const Node & node, std::string_view text, size_t pos, size_t count, const Continuation * next, size_t & end) const;

    std::vector<std::unique_ptr<CharacterSet>> sets;
    std::unique_ptr<Node> root;
};

using BPEPatternPtr = std::shared_ptr<const BPEPattern>;

}
