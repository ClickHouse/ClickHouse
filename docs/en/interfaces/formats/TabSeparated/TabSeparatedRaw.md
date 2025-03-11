---
title: TabSeparatedRaw
slug: /interfaces/formats/TabSeparatedRaw
keywords: [TabSeparatedRaw]
input_format: true
output_format: true
alias: ['TSVRaw', 'Raw']
---

| Input | Output | Alias           |
|-------|--------|-----------------|
| ✔     | ✔      | `TSVRaw`, `Raw` |

## Description {#description}

Differs from the [`TabSeparated`](/interfaces/formats/TabSeparated) format in that rows are written without escaping.

:::note
When parsing with this format, tabs or line-feeds are not allowed in each field.
:::

For a comparison of the `TabSeparatedRaw` format and the `RawBlob` format see: [Raw Formats Comparison](../RawBLOB.md/#raw-formats-comparison)

## Example Usage {#example-usage}

## Format Settings {#format-settings}

