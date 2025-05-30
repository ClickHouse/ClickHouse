---
description: 'System table containing a list of Unicode characters and their properties.'
keywords: ['system table', 'unicode']
slug: /operations/system-tables/unicode
title: 'system.unicode'
---
# system.unicode

The `system.unicode` table is a virtual table that provides information about Unicode characters and their properties(https://unicode-org.github.io/icu/userguide/strings/properties.html). This table is generated on-the-fly.

Columns

- `code_point` ([String](../../sql-reference/data-types/string.md)) — The UTF-8 representation of the code point.
- `code_point_value` ([Int32](../../sql-reference/data-types/int-uint.md)) — The numeric value of the code point.
- `notation` ([String](../../sql-reference/data-types/string.md)) — The Unicode notation of the code point.
- Binary Properties ([UInt8](../../sql-reference/data-types/int-uint.md)) - The binary properties of the code point.
    - `Alphabetic`, `ASCII_Hex_Digit`, `Case_Ignorable`...
- Enumerated Properties ([Int32](../../sql-reference/data-types/int-uint.md)) - The enumerated properties of the code point.
    - `Bidi_Class`, `Bidi_Paired_Bracket_Type`, `Block`...
- String Properties ([String](../../sql-reference/data-types/string.md)) - The string properties(ASCII String or Unicode String) of the code point
    - `Case_Folding`, `Decomposition_Mapping`, `Name`...
- `Numeric_Value` ([Float64](../../sql-reference/data-types/float.md)) - The numeric value of the code point.
- `Script_Extensions` ([Array(Int32)](../../sql-reference/data-types/array.md)) - The script extensions of the code point.
- `General_Category_Mask` ([Int32](../../sql-reference/data-types/int-uint.md)) - The general category mask of the code point.


**Example**
```sql
SELECT * FROM system.unicode WHERE code_point = 'a' LIMIT 1;
```

```text
Row 1:
──────
code_point:                      a
code_point_value:                97
notation:                        U+0061
Alphabetic:                      1
ASCII_Hex_Digit:                 1
Bidi_Control:                    0
Bidi_Mirrored:                   0
Dash:                            0
Default_Ignorable_Code_Point:    0
Deprecated:                      0
Diacritic:                       0
Extender:                        0
Full_Composition_Exclusion:      0
Grapheme_Base:                   1
Grapheme_Extend:                 0
Grapheme_Link:                   0
Hex_Digit:                       1
Hyphen:                          0
ID_Continue:                     1
ID_Start:                        1
Ideographic:                     0
IDS_Binary_Operator:             0
IDS_Trinary_Operator:            0
Join_Control:                    0
Logical_Order_Exception:         0
Lowercase:                       1
Math:                            0
Noncharacter_Code_Point:         0
Quotation_Mark:                  0
Radical:                         0
Soft_Dotted:                     0
Terminal_Punctuation:            0
Unified_Ideograph:               0
Uppercase:                       0
White_Space:                     0
XID_Continue:                    1
XID_Start:                       1
Case_Sensitive:                  1
Sentence_Terminal:               0
Variation_Selector:              0
NFD_Inert:                       1
NFKD_Inert:                      1
NFC_Inert:                       0
NFKC_Inert:                      0
Segment_Starter:                 1
Pattern_Syntax:                  0
Pattern_White_Space:             0
alnum:                           1
blank:                           0
graph:                           1
print:                           1
xdigit:                          1
Cased:                           1
Case_Ignorable:                  0
Changes_When_Lowercased:         0
Changes_When_Uppercased:         1
Changes_When_Titlecased:         1
Changes_When_Casefolded:         0
Changes_When_Casemapped:         1
Changes_When_NFKC_Casefolded:    0
Emoji:                           0
Emoji_Presentation:              0
Emoji_Modifier:                  0
Emoji_Modifier_Base:             0
Emoji_Component:                 0
Regional_Indicator:              0
Prepended_Concatenation_Mark:    0
Extended_Pictographic:           0
Basic_Emoji:                     0
Emoji_Keycap_Sequence:           0
RGI_Emoji_Modifier_Sequence:     0
RGI_Emoji_Flag_Sequence:         0
RGI_Emoji_Tag_Sequence:          0
RGI_Emoji_ZWJ_Sequence:          0
RGI_Emoji:                       0
IDS_Unary_Operator:              0
ID_Compat_Math_Start:            0
ID_Compat_Math_Continue:         0
Bidi_Class:                      0
Block:                           1
Canonical_Combining_Class:       0
Decomposition_Type:              0
East_Asian_Width:                4
General_Category:                2
Joining_Group:                   0
Joining_Type:                    0
Line_Break:                      2
Numeric_Type:                    0
Script:                          25
Hangul_Syllable_Type:            0
NFD_Quick_Check:                 1
NFKD_Quick_Check:                1
NFC_Quick_Check:                 1
NFKC_Quick_Check:                1
Lead_Canonical_Combining_Class:  0
Trail_Canonical_Combining_Class: 0
Grapheme_Cluster_Break:          0
Sentence_Break:                  4
Word_Break:                      1
Bidi_Paired_Bracket_Type:        0
Indic_Positional_Category:       0
Indic_Syllabic_Category:         0
Vertical_Orientation:            0
Identifier_Status:               1
General_Category_Mask:           4
Numeric_Value:                   0
Age:                             1.1
Bidi_Mirroring_Glyph:            a
Case_Folding:                    a
ISO_Comment:                     
Lowercase_Mapping:               a
Name:                            LATIN SMALL LETTER A
Simple_Case_Folding:             a
Simple_Lowercase_Mapping:        a
Simple_Titlecase_Mapping:        
Simple_Uppercase_Mapping:        
Titlecase_Mapping:               
Unicode_1_Name:                  
Uppercase_Mapping:               
Bidi_Paired_Bracket:             
Script_Extensions:               [25]

```

```sql
SELECT code_point, code_point_value, notation FROM system.unicode WHERE code_point = '😂';
```
```text
   ┌─code_point─┬─code_point_value─┬─notation─┐
1. │ 😂          │           128514 │ U+1F602  │
   └────────────┴──────────────────┴──────────┘
```
