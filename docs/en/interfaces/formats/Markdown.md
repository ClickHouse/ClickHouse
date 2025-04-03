---
description: 'Documentation for the Markdown format'
keywords: ['Markdown']
slug: /interfaces/formats/Markdown
title: 'Markdown'
---

## Description {#description}

You can export results using [Markdown](https://en.wikipedia.org/wiki/Markdown) format to generate output ready to be pasted into your `.md` files:

The markdown table will be generated automatically and can be used on markdown-enabled platforms, like Github. This format is used only for output.

## Example Usage {#example-usage}

```sql
SELECT
    number,
    number * 2
FROM numbers(5)
FORMAT Markdown
```
```results
| number | multiply(number, 2) |
|-:|-:|
| 0 | 0 |
| 1 | 2 |
| 2 | 4 |
| 3 | 6 |
| 4 | 8 |
```

## Format Settings {#format-settings}