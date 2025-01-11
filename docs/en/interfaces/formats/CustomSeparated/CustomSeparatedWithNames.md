---
title : CustomSeparatedWithNames
slug : /en/interfaces/formats/CustomSeparatedWithNames
keywords : [CustomSeparatedWithNames]
---

## Description

Also prints the header row with column names, similar to [TabSeparatedWithNames](/docs/en/interfaces/formats/TabSeparatedWithNames).

## Example Usage

## Format Settings

:::note
If setting [input_format_with_names_use_header](/docs/en/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to 1,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/docs/en/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
Otherwise, the first row will be skipped.
:::