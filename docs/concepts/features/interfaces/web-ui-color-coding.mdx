---
description: 'Documentation for the per-column color coding modes in the built-in Web SQL UI (`/play`)'
sidebarTitle: 'Web UI Color Coding'
slug: /interfaces/web-ui-color-coding
title: 'Web UI Color Coding'
doc_type: 'reference'
sidebar: false
---

The built-in Web SQL UI (`play.html`, served at the [`/play`](/concepts/features/interfaces/http) path of any ClickHouse HTTP port) can color result cells to make patterns in a column easier to spot at a glance. Each column has its own, independently switchable color coding mode.

## Switching the mode {#switching-the-mode}

A 🌈 icon appears at the right of every column header. Click it to cycle the column through its available modes. On devices with a hovering pointer (a mouse) the icon is shown only while the header is hovered, so it stays out of the way otherwise; on touch and other coarse-pointer devices, which have no hover, the icon is always shown so it can be tapped directly.

The set of modes a column offers depends on its type:

- Numeric columns and `Date`/`DateTime`/`Date32`/`DateTime64` columns cycle through `bar` → `heatmap` → `categorical` → `none`.
- All other columns toggle between `none` and `categorical`.

The default mode is `bar` for numeric columns and `none` for every other column, including date and time columns.

## Modes {#modes}

- **`bar`** — draws a horizontal bar in the cell proportional to the value. For numeric columns the bar grows from a zero baseline; for `Date`/`DateTime` columns it spans the column's `min`..`max` range instead, since a zero baseline is not meaningful for timestamps.
- **`heatmap`** — fills the whole cell background with a color that encodes the value scaled between the column's minimum and maximum.
- **`categorical`** — fills the cell background with a color derived by hashing the cell's value, so equal values get the same color and different values get different colors. This works for any column type.
- **`none`** — no color coding.

`Date`, `DateTime`, `Date32`, and `DateTime64` columns are colored by their time value, parsed in UTC so the scale is independent of the viewer's browser timezone.

The `heatmap` and `categorical` background colors use the `oklch` color space, varying only the hue while keeping the lightness and chroma fixed per theme, so the cell text stays readable in both the light and dark themes. The background fills the entire cell even when a row spans more than one line.

## Categorical emphasis on selection {#categorical-emphasis}

In `categorical` mode, selecting a cell emphasizes the other cells that share the same value, drawing them with a heavier font and a full-contrast text color (pure white in the dark theme, pure black in the light theme). The selected cell itself is left unemphasized. This makes it easy to see where else a particular value occurs in the column.

## Persistence {#persistence}

The chosen modes are remembered per column in the page URL and in the browser history, so reloading the page, sharing the link, or navigating back and forward preserves them. Only non-default choices are stored, to keep the URL and history state compact.

## Limitations {#limitations}

- The vertical (transposed) single-row layout shows no color coding.
- `DateTime64(9)` differences below a microsecond are not distinguished in the color scale, which is not visually meaningful for a gradient.
