---
title : PrettyNoEscapes
slug : /en/interfaces/formats/PrettyNoEscapes
keywords : [PrettyNoEscapes]
input_format: false
output_format: true
alias: []
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description

Differs from [Pretty](/docs/en/interfaces/formats/Pretty) in that [ANSI-escape sequences](http://en.wikipedia.org/wiki/ANSI_escape_code) aren’t used. 
This is necessary for displaying the format in a browser, as well as for using the ‘watch’ command-line utility.

## Example Usage

Example:

```bash
$ watch -n1 "clickhouse-client --query='SELECT event, value FROM system.events FORMAT PrettyCompactNoEscapes'"
```

:::note
The [HTTP interface](../../../interfaces/http.md) can be used for displaying this format in the browser.
:::

## Format Settings

<PrettyFormatSettings/>