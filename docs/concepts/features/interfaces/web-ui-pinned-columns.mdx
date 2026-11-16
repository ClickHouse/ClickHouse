---
description: 'Documentation for pinned (sticky) columns in the built-in Web SQL UI (`/play`)'
sidebarTitle: 'Web UI Pinned Columns'
slug: /interfaces/web-ui-pinned-columns
title: 'Web UI Pinned Columns'
doc_type: 'reference'
sidebar: false
---

The built-in Web SQL UI (`play.html`, served at the [`/play`](/concepts/features/interfaces/http) path of any ClickHouse HTTP port) can keep chosen columns of a result visible while the rest of the table is scrolled sideways. A pinned column sticks to the near edge of the result area instead of scrolling away, so a key column — an identifier, a name, a date — stays in view next to whichever far-right columns you are reading.

## Pinning a column {#pinning-a-column}

A 📌 pushpin icon appears at the left of every column header. Click it to pin the column; click it again to unpin it. The icon is a real button, so keyboard users can tab to it and toggle it with the keyboard, and it exposes its pinned state to assistive technology.

On devices with a hovering pointer (a mouse) the icon is shown only while the header is hovered or the toggle is focused, so it stays out of the way otherwise; on touch and other coarse-pointer devices, which have no hover, the icon is always shown so it can be tapped directly. A pinned column keeps its icon shown at full strength as an indicator that the column is pinned.

## When pinning is available {#when-pinning-is-available}

Pinning is offered only when the table is wide enough to scroll horizontally. When the whole table fits in the viewport there is nothing to scroll away from, so the pin toggle is disabled and excluded from the keyboard tab order. As soon as the result becomes horizontally scrollable — a wide result, a narrow window, or a horizontally growing streamed result — the toggles become active.

## How a pinned column behaves when scrolling {#how-a-pinned-column-behaves-when-scrolling}

A pinned column slides with the content during horizontal scrolling until it would leave the viewport, at which point it sticks to the near edge — the left edge for columns on the left, the right edge for columns on the right — and stays visible. Several pinned columns stack against the edge in their original order, each delimited from the scrolling content by a border. A pinned cell paints over the ordinary cells scrolling underneath it, including a selected cell, so it always stays readable.

## Shared across results {#shared-across-results}

Pins are tracked by column name and shared across every result table on the page, the same way the per-column [color coding modes](/concepts/features/interfaces/web-ui-color-coding) are. Pinning a column named `id` in one result keeps a column named `id` pinned in the other results and in later queries that return the same column.

## Persistence {#persistence}

The pinned set is remembered in the page URL, in the browser history, and in the per-tab result snapshot, so reloading the page, sharing the link, or navigating back and forward preserves it. Only pinned columns are stored — an empty set adds nothing to the URL or history state — to keep them compact.

## Limitations {#limitations}

- The vertical (transposed) single-row layout has no horizontally scrolling columns, so it offers no pinning.
