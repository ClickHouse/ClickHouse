---
title : Form
slug : /en/interfaces/formats/Form
keywords : [Form]
input_format: true
output_format: false
alias: []
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✗      |       |


## Description

The `Form` format can be used to read a single record in the application/x-www-form-urlencoded format 
in which data is formatted as `key1=value1&key2=value2`.

## Example Usage

Given a file `data.tmp` placed in the `user_files` path with some URL encoded data:

```text title="data.tmp"
t_page=116&c.e=ls7xfkpm&c.tti.m=raf&rt.start=navigation&rt.bmr=390%2C11%2C10
```

```sql title="Query"
SELECT * FROM file(data.tmp, Form) FORMAT vertical;
```

```response title="Response"
Row 1:
──────
t_page:   116
c.e:      ls7xfkpm
c.tti.m:  raf
rt.start: navigation
rt.bmr:   390,11,10
```

## Format Settings
