---
title : PrettySpaceNoEscapes
slug : /en/interfaces/formats/PrettySpaceNoEscapes
keywords : [PrettySpaceNoEscapes]
input_format: false
output_format: true
alias: []
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description

Differs from the [`PrettySpace`](./PrettySpace.md) format in that [ANSI-escape sequences](http://en.wikipedia.org/wiki/ANSI_escape_code) are not used. 
This is necessary for displaying this format in a browser, as well as for using the ‘watch’ command-line utility.

## Example Usage

## Format Settings

<PrettyFormatSettings/>