---
alias: []
description: 'Documentation for the PrettyNoEscapesMonoBlock format'
input_format: false
keywords: ['PrettyNoEscapesMonoBlock']
output_format: true
slug: /interfaces/formats/PrettyNoEscapesMonoBlock
title: 'PrettyNoEscapesMonoBlock'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

Differs from the [`PrettyNoEscapes`](./PrettyNoEscapes.md) format in that up to `10,000` rows are buffered, 
and then output as a single table, and not by blocks.

## Example Usage {#example-usage}

## Format Settings {#format-settings}

<PrettyFormatSettings/>