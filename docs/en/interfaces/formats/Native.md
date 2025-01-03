---
title : Native
slug : /en/interfaces/formats/Native
keywords : [Native]
---

## Description

The most efficient format. Data is written and read by blocks in binary format. For each block, the number of rows, number of columns, column names and types, and parts of columns in this block are recorded one after another. In other words, this format is “columnar” – it does not convert columns to rows. 
This is the format used in the native interface for interaction between servers, for using the command-line client, and for C++ clients.
You can use this format to quickly generate dumps that can only be read by the ClickHouse DBMS. It does not make sense to work with this format yourself.

## Example Usage

## Format Settings