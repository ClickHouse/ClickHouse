---
title : PrettyCompactMonoBlock
slug: /interfaces/formats/PrettyCompactMonoBlock
keywords : [PrettyCompactMonoBlock]
input_format: false
output_format: true
alias: []
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

Differs from the [`PrettyCompact`](./PrettyCompact.md) format in that up to `10,000` rows are buffered, 
and then output as a single table, and not by [blocks](../../../development/architecture.md/#block-block).

## Example Usage {#example-usage}

## Format Settings {#format-settings}

<PrettyFormatSettings/>