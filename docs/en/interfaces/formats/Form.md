---
title : Form
slug : /en/interfaces/formats/Form
keywords : [Form]
---

## Description

The Form format can be used to read or write a single record in the application/x-www-form-urlencoded format in which data is formatted `key1=value1&key2=value2`.

## Example Usage

Given a file `data.tmp` placed in the `user_files` path with some URL encoded data:

```text
t_page=116&c.e=ls7xfkpm&c.tti.m=raf&rt.start=navigation&rt.bmr=390%2C11%2C10
```

```sql
SELECT * FROM file(data.tmp, Form) FORMAT vertical;
```

Result:

```text
Row 1:
──────
t_page:   116
c.e:      ls7xfkpm
c.tti.m:  raf
rt.start: navigation
rt.bmr:   390,11,10
```

## Format Settings
