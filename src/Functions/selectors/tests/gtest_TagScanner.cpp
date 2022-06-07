#include <Functions/selectors/TagScanner.h>
#include <gtest/gtest.h>

using namespace DB;
using namespace std::literals::string_view_literals;

TEST(TagScanner, Scan)
{
    std::string s = R"(
<div class="code-example">
<span class="token property">color</span>
        blue
    <span class="token punctuation">;</span>
</div>
)";
    const char * begin = s.data();
    const char * end = s.data() + s.size();

    std::vector<TagPreview> expected_tags = {
        TagPreview{"div"sv, .is_closing = false},
        TagPreview{"span"sv, .is_closing = false},
        TagPreview{"span"sv, .is_closing = true},
        TagPreview{"span"sv, .is_closing = false},
        TagPreview{"span"sv, .is_closing = true},
        TagPreview{"div"sv, .is_closing = true},
    };

    TagScanner tag_scanner;
    size_t ind = 0;
    TagPreview tag;
    begin = tag_scanner.scan(begin, end, tag);
    for (; begin != end; begin = tag_scanner.scan(begin, end, tag))
    {
        EXPECT_EQ(tag, expected_tags[ind++]);
    }

    EXPECT_EQ(ind, expected_tags.size());
}
