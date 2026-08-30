#include <gtest/gtest.h>

#include <Common/OptimizedRegularExpression.h>

TEST(OptimizeRE, analyze)
{
    auto test_f = [](const std::string & regexp,
                     const std::string & required,
                     std::vector<std::string> expect_alternatives = {},
                     bool trival_expected = false,
                     bool has_capture_expected = false,
                     bool prefix_expected = false)
    {
        auto [answer, is_trivial, has_capture, is_prefix, alternatives, match_kind]
            = DB::OptimizedRegularExpression::analyze(regexp);
        std::cerr << regexp << std::endl;
        EXPECT_EQ(required, answer);
        EXPECT_EQ(alternatives, expect_alternatives);
        EXPECT_EQ(is_trivial, trival_expected);
        EXPECT_EQ(has_capture, has_capture_expected);
        EXPECT_EQ(is_prefix, prefix_expected);
        /// An anchored kind means the required substring is the whole literal, so it cannot be empty.
        EXPECT_TRUE(!DB::isAnchoredLiteralMatchKind(match_kind) || !answer.empty());
    };
    test_f("abc", "abc", {}, true, false, true);
    test_f("c([^k]*)de", "", {}, false, true, false);
    test_f("a|XYZ", "", {"a", "XYZ"});
    test_f("XYZ|a", "", {"XYZ", "a"});
    test_f("[Ff]|XYZ", "", {"", "XYZ"});
    test_f("XYZ|[Ff]", "", {"XYZ", ""});
    test_f("XYZ|ABC|[Ff]", "", {"XYZ", "ABC", ""});
    test_f("(?-s)bob", "bob", {}, false, true, true);
    test_f("(?s)bob", "bob", {}, false, true, true);
    test_f("(?ssss", "");
    test_f("[asdf]ss(?:ss)ss", "ssssss");
    test_f("abc(de)fg", "abcdefg", {}, false, true, true);
    test_f("abc(de|xyz)fg", "abc", {"abcdefg", "abcxyzfg"}, false, true, true);
    test_f("abc(de?f|xyz)fg", "abc", {"abcd", "abcxyzfg"}, false, true, true);
    test_f("abc|fgk|xyz", "", {"abc","fgk", "xyz"});
    test_f("(abc)", "abc", {}, false, true, true);
    test_f("(abc|fgk)", "", {"abc","fgk"}, false, true, false);
    test_f("(abc|fgk)(e|f|zkh|)", "", {"abc","fgk"}, false, true, false);
    test_f("abc(abc|fg)xyzz", "xyzz", {"abcabcxyzz","abcfgxyzz"}, false, true, false);
    test_f("((abc|fg)kkk*)xyzz", "xyzz", {"abckk", "fgkk"}, false, true, false);
    test_f("abc(*(abc|fg)*)xyzz", "xyzz", {}, false, true, false);
    test_f("abc[k]xyzz", "xyzz");
    test_f("(abc[k]xyzz)", "xyzz", {}, false, true, false);
    test_f("abc((de)fg(hi))jk", "abcdefghijk", {}, false, true, true);
    test_f("abc((?:de)fg(?:hi))jk", "abcdefghijk", {}, false, true, true);
    test_f("abc((de)fghi+zzz)jk", "abcdefghi", {}, false, true, true);
    test_f("abc((de)fg(hi))?jk", "abc", {}, false, true, true);
    test_f("abc((de)fghi?zzz)jk", "abcdefgh", {}, false, true, true);
    test_f("abc(*cd)jk", "cdjk", {}, false, true, false);
    test_f(R"(abc(de|xyz|(\{xx\}))fg)", "abc", {"abcdefg", "abcxyzfg", "abc{xx}fg"}, false, true, true);
    test_f("abc(abc|fg)?xyzz", "xyzz", {}, false, true, false);
    test_f("abc(abc|fg){0,1}xyzz", "xyzz", {}, false, true, false);
    test_f("abc(abc|fg)xyzz|bcdd?k|bc(f|g|h?)z", "", {"abcabcxyzz", "abcfgxyzz", "bcd", "bc"});
    test_f("abc(abc|fg)xyzz|bc(dd?x|kk?y|(f))k|bc(f|g|h?)z", "", {"abcabcxyzz", "abcfgxyzz", "bcd", "bck", "bcfk", "bc"}, false, true, false);
    test_f("((?:abc|efg|xyz)/[a-zA-Z0-9]{1-50})(/?[^ ]*|)", "", {"abc/", "efg/", "xyz/"}, false, true, false);
    test_f(R"([Bb]ai[Dd]u[Ss]pider(?:-[A-Za-z]{1,30})(?:-[A-Za-z]{1,30}|)|bingbot|\bYeti(?:-[a-z]{1,30}|)|Catchpoint(?: bot|)|[Cc]harlotte|Daumoa(?:-feedfetcher|)|(?:[a-zA-Z]{1,30}-|)Googlebot(?:-[a-zA-Z]{1,30}|))", "", {"pider-", "bingbot", "Yeti-", "Yeti", "Catchpoint bot", "Catchpoint", "harlotte", "Daumoa-feedfetcher", "Daumoa", "-Googlebot", "Googlebot"});
    test_f("abc|(:?xx|yy|zz|x?)def", "", {"abc", "def"});
    test_f("abc|(:?xx|yy|zz|x?){1,2}def", "", {"abc", "def"});
    test_f(R"(\\A(?:(?:[-0-9_a-z]+(?:\\.[-0-9_a-z]+)*)/k8s1)\\z)", "/k8s1");
    test_f("[a-zA-Z]+(?P<num>\\d+)", "", {}, false, true, false);
    test_f("[a-zA-Z]+(?<num>\\d+)", "", {}, false, true, false);
    test_f("[a-zA-Z]+(?'num'\\d+)", "", {}, false, true, false);
    test_f("[a-zA-Z]+(?x<num>\\d+)", "x<num>", {}, false, true, false);
    /// Escapes that encode a byte through trailing argument characters must not leak those
    /// characters into the required substring; otherwise the strstr pre-filter would drop
    /// matching rows before re2 runs. See https://github.com/ClickHouse/ClickHouse/issues/106382.
    test_f("\\x41bc", "");           /// `\x41` is 'A'; "41bc" must not become a required substring
    test_f("\\101aa", "");           /// octal `\101` is 'A'; "01aa" must not become required
    test_f("\\x41abcd", "abcd");     /// only two hex digits belong to `\x41`; "abcd" stays literal
    test_f("\\x41abc", "abc");       /// literal tail after the escape is still extracted
    test_f("\\x{41}bcd", "bcd");     /// `\x{...}` form is consumed whole
    test_f("\\x41\\x42cd", "");      /// consecutive hex escapes leave only the short tail "cd"
    test_f("\\x41(bc)", "", {}, false, true, false); /// escape before a capturing group
    test_f("\\pLabc", "abc");        /// one-letter Unicode property `\pL`; "Labc" must not be required
    test_f("\\PNabc", "abc");        /// negated one-letter property `\PN`
    test_f("\\p{L}abc", "abc");      /// braced Unicode property `\p{...}`
    test_f("\\P{N}abc", "abc");      /// braced negated Unicode property `\P{...}`
    test_f("\\Qa(b)c\\E", "");       /// `\Q...\E` body is literal; "abc" must not be required
    test_f("\\Qa(b)c\\Exyz", "xyz"); /// extraction resumes after the closing `\E`
    /// A NUL byte (`\0`) in the pattern is an ordinary literal (re2 matches it), not a terminator;
    /// the analyzer must not stop at it, otherwise the strstr pre-filter wrongly drops every row.
    test_f(std::string("a\0b", 3), std::string("a\0b", 3), {}, true, false, true); /// literal NUL kept in the required substring
    test_f(std::string("\0*abc", 5), "abc", {}, false, false, false);             /// `\0*` is optional; "abc" stays the required substring
    test_f(std::string("\0*@(.*)$", 8), "", {}, false, true, false);              /// the analysis must not stop at the leading NUL
}

TEST(OptimizeRE, anchoredLiteral)
{
    auto test_f = [](const std::string & regexp, DB::RegexpMatchKind kind_expected, const std::string & literal_expected = "")
    {
        auto result = DB::OptimizedRegularExpression::analyze(regexp);
        std::cerr << regexp << std::endl;
        EXPECT_EQ(result.match_kind, kind_expected);
        if (DB::isAnchoredLiteralMatchKind(result.match_kind))
            EXPECT_EQ(result.required_substring, literal_expected);
    };

    using Kind = DB::RegexpMatchKind;

    test_f("^abc", Kind::Prefix, "abc");
    test_f("abc$", Kind::Suffix, "abc");
    test_f("^abc$", Kind::Exact, "abc");
    /// The required substring is the literal to compare against, and the analysis drops short and tuned-away ones.
    test_f("^a", Kind::General);
    test_f("a$", Kind::General);
    test_f("^ab$", Kind::General);
    test_f("^www", Kind::General);
    test_f("^a\\.c$", Kind::Exact, "a.c");
    test_f("a\\$b$", Kind::Suffix, "a$b");
    test_f("\\^abc$", Kind::Suffix, "^abc");
    test_f(std::string("^a\0b$", 5), Kind::Exact, std::string("a\0b", 3));

    /// Not anchored literals.
    test_f("abc", Kind::Substring);                 /// a plain literal is matched anywhere
    test_f("", Kind::Substring);
    test_f("^", Kind::General);                     /// the empty literal is left to re2
    test_f("$", Kind::General);
    test_f("^$", Kind::General);
    test_f("^a.c", Kind::General);                  /// `.` is not a literal
    test_f("^abc.*", Kind::General);
    test_f("^ab|cd$", Kind::General);
    test_f("^(abc)$", Kind::General);
    test_f("a$b", Kind::General);                   /// `$` in the middle is an assertion, not an anchor
    test_f("a^b", Kind::General);
    test_f("^ab\\d", Kind::General);                /// an escape which is not an escaped metacharacter
    test_f("^ab\\", Kind::General);                 /// a dangling escape
    test_f("^\\Qa.c\\E$", Kind::General);
}

TEST(OptimizeRE, anchoredLiteralIsCaseSensitiveOnly)
{
    const DB::OptimizedRegularExpression case_sensitive("^abc");
    EXPECT_EQ(case_sensitive.getMatchKind(), DB::RegexpMatchKind::Prefix);
    EXPECT_EQ(case_sensitive.getRE2(), nullptr);
    EXPECT_TRUE(case_sensitive.match("abcdef", 6));
    EXPECT_FALSE(case_sensitive.match("ABCdef", 6));

    /// A case-insensitive re2 folds Unicode, which a byte comparison cannot reproduce, so the pattern keeps
    /// using re2 and gets no anchored kind.
    const DB::OptimizedRegularExpression case_insensitive("^abc", DB::OptimizedRegularExpression::RE_CASELESS);
    EXPECT_EQ(case_insensitive.getMatchKind(), DB::RegexpMatchKind::General);
    EXPECT_NE(case_insensitive.getRE2(), nullptr);
    EXPECT_TRUE(case_insensitive.match("ABCdef", 6));
    EXPECT_FALSE(case_insensitive.match("xABCdef", 7));
}
