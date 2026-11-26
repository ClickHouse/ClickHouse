url: https://github.com/ridiculousfish/widecharwidth
commit: 62edda3d435392bf84c7db59f01f53370bbb1715

widecharwidth is a Python script that outputs a C++11 header-only implementation of `wcwidth()`, by downloading and parsing the latest `UnicodeData.txt`, `EastAsianWidth.txt`, and `emoji-data.txt`.

## Usage

You may directly copy and use the included `widechar_width.h`.

This header contains a single public function `widechar_wcwidth()`. This returns either a positive width value (1 or 2), or a negative value such as `widechar_private_use`. Note that there are several possible negative return values, to distinguish among different scenarios.

If you aren't sure how to handle negative return values, try this table:

 | return value            | width |
 |-------------------------|---|
 | `widechar_nonprint`     | 0 |
 | `widechar_combining`    | 0 |
 | `widechar_ambiguous`    | 1 |
 | `widechar_private_use`  | 1 |
 | `widechar_unassigned`   | 0 |
 | `widechar_widened_in_9` | 2 (or maybe 1, renderer dependent) |

## Regenerating the header

To regenerate the header, run `make`. This will download and parse the relevant files, and run tests.

## License

widecharwidth and its output headers are released into the public domain. They may be used for any purpose without requiring attribution, or under the CC0 license if public domain is not available. See included LICENSE.

