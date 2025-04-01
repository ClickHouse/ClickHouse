---
description: 'Creates an array from different argument values.'
sidebar_position: 154
slug: /sql-reference/aggregate-functions/reference/groupuniqarray
title: 'groupUniqArray'
---

# groupUniqArray

Syntax: `groupUniqArray(x)` or `groupUniqArray(max_size)(x)`

Creates an array from different argument values. Memory consumption is the same as for the [uniqExact](../../../sql-reference/aggregate-functions/reference/uniqexact.md) function.

The second version (with the `max_size` parameter) limits the size of the resulting array to `max_size` elements.
For example, `groupUniqArray(1)(x)` is equivalent to `[any(x)]`.
