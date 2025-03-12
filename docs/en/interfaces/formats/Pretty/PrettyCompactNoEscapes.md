---
title: 'PrettyCompactNoEscapes'
slug: /interfaces/formats/PrettyCompactNoEscapes
keywords: [PrettyCompactNoEscapes]
input_format: false
output_format: true
alias: []
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

Differs from the [`PrettyCompact`](./PrettyCompact.md) format in that [ANSI-escape sequences](http://en.wikipedia.org/wiki/ANSI_escape_code) aren't used. 
This is necessary for displaying the format in a browser, as well as for using the 'watch' command-line utility.

## Example Usage {#example-usage}

## Format Settings {#format-settings}

<PrettyFormatSettings/>
