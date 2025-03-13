---
title: 'PrettySpaceNoEscapesMonoBlock'
slug: /interfaces/formats/PrettySpaceNoEscapesMonoBlock
keywords: ['PrettySpaceNoEscapesMonoBlock']
input_format: false
output_format: true
alias: []
description: 'Documentation for the PrettySpaceNoEscapesMonoBlock format'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

Differs from the [`PrettySpaceNoEscapes`](./PrettySpaceNoEscapes.md) format in that up to `10,000` rows are buffered, 
and then output as a single table, and not by [blocks](/development/architecture#block).

## Example Usage {#example-usage}

## Format Settings {#format-settings}

<PrettyFormatSettings/>