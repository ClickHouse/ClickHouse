---
alias: []
description: 'Documentation for the PrettyNoEscapes format'
input_format: false
keywords: ['PrettyNoEscapes']
output_format: true
slug: /interfaces/formats/PrettyNoEscapes
title: 'PrettyNoEscapes'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

Differs from [Pretty](/interfaces/formats/Pretty) in that [ANSI-escape sequences](http://en.wikipedia.org/wiki/ANSI_escape_code) aren't used. 
This is necessary for displaying the format in a browser, as well as for using the 'watch' command-line utility.

## Example usage {#example-usage}

Example:

```bash
$ watch -n1 "clickhouse-client --query='SELECT event, value FROM system.events FORMAT PrettyCompactNoEscapes'"
```

:::note
The [HTTP interface](../../../interfaces/http.md) can be used for displaying this format in the browser.
:::

## Format settings {#format-settings}

<PrettyFormatSettings/>