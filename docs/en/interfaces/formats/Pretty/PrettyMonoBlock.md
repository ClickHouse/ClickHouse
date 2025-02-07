---
title : PrettyMonoBlock
slug : /en/interfaces/formats/PrettyMonoBlock
keywords : [PrettyMonoBlock]
input_format: false
output_format: true
alias: []
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description

Differs from the [`Pretty`](/docs/en/interfaces/formats/Pretty) format in that up to `10,000` rows are buffered,
and then output as a single table, and not by [blocks](../../../development/architecture.md/#block-block).

## Example Usage

## Format Settings

<PrettyFormatSettings/>