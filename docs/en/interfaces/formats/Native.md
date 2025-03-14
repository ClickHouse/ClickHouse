---
alias: []
description: 'Documentation for the Native format'
input_format: true
keywords: ['Native']
output_format: true
slug: /interfaces/formats/Native
title: 'Native'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

The `Native` format is ClickHouse's most efficient format because it is truly "columnar" 
in that it does not convert columns to rows.  

In this format data is written and read by [blocks](/development/architecture#block) in a binary format. 
For each block, the number of rows, number of columns, column names and types, and parts of columns in the block are recorded one after another. 

This is the format used in the native interface for interaction between servers, for using the command-line client, and for C++ clients.

:::tip
You can use this format to quickly generate dumps that can only be read by the ClickHouse DBMS. 
It might not be practical to work with this format yourself.
:::

## Example Usage {#example-usage}

## Format Settings {#format-settings}