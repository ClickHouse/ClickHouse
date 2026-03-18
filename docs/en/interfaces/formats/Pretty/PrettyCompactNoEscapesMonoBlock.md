---
alias: []
description: 'Documentation for the PrettyCompactNoEscapesMonoBlock format'
input_format: false
keywords: ['PrettyCompactNoEscapesMonoBlock']
output_format: true
slug: /interfaces/formats/PrettyCompactNoEscapesMonoBlock
title: 'PrettyCompactNoEscapesMonoBlock'
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