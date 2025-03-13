---
title: 'PrettyCompactNoEscapesMonoBlock'
slug: /interfaces/formats/PrettyCompactNoEscapesMonoBlock
keywords: ['PrettyCompactNoEscapesMonoBlock']
input_format: false
output_format: true
alias: []
description: 'Documentation for the PrettyCompactNoEscapesMonoBlock format'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

Differs from the [`PrettyCompactNoEscapes`](./PrettyCompactNoEscapes.md) format in that up to `10,000` rows are buffered, 
and then output as a single table, and not by [blocks](/development/architecture#block).

## Example Usage {#example-usage}

## Format Settings {#format-settings}

<PrettyFormatSettings/>