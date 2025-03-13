---
title: 'TabSeparatedRawWithNames'
slug: /interfaces/formats/TabSeparatedRawWithNames
keywords: ['TabSeparatedRawWithNames', 'TSVRawWithNames', 'RawWithNames']
input_format: true
output_format: true
alias: ['TSVRawWithNames', 'RawWithNames']
description: 'Documentation for the TabSeparatedRawWithNames format'
---

| Input | Output | Alias                             |
|-------|--------|-----------------------------------|
| ✔     | ✔      | `TSVRawWithNames`, `RawWithNames` |

## Description {#description}

Differs from the [`TabSeparatedWithNames`](./TabSeparatedWithNames.md) format, 
in that the rows are written without escaping.

:::note
When parsing with this format, tabs or line-feeds are not allowed in each field.
:::

## Example Usage {#example-usage}

## Format Settings {#format-settings}