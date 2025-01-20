---
title : PrettyNoEscapes
slug : /en/interfaces/formats/PrettyNoEscapes
keywords : [PrettyNoEscapes]
---

## Description

Differs from [Pretty](/docs/en/interfaces/formats/Pretty) in that ANSI-escape sequences aren’t used. This is necessary for displaying this format in a browser, as well as for using the ‘watch’ command-line utility.

## Example Usage

Example:

``` bash
$ watch -n1 "clickhouse-client --query='SELECT event, value FROM system.events FORMAT PrettyCompactNoEscapes'"
```

You can use the HTTP interface for displaying in the browser.

## Format Settings