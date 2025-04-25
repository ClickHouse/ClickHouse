---
description: 'Documentation for uniqTheta Functions'
sidebar_label: 'uniqTheta'
sidebar_position: 210
slug: /sql-reference/functions/uniqtheta-functions
title: 'uniqTheta Functions'
---

# uniqTheta Functions

uniqTheta functions work for two uniqThetaSketch objects to do set operation calculations such as  ∪ / ∩ / × (union/intersect/not), it is to return a new uniqThetaSketch object contain the result.

A uniqThetaSketch object is to be constructed by aggregation function uniqTheta with -State.

UniqThetaSketch is a data structure storage of approximate values set.
For more information on RoaringBitmap, see: [Theta Sketch Framework](https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html).

## uniqThetaUnion {#uniqthetaunion}

Two uniqThetaSketch objects to do union calculation(set operation ∪), the result is a new uniqThetaSketch.

```sql
uniqThetaUnion(uniqThetaSketch,uniqThetaSketch)
```

**Arguments**

- `uniqThetaSketch` – uniqThetaSketch object.

**Example**

```sql
select finalizeAggregation(uniqThetaUnion(a, b)) as a_union_b, finalizeAggregation(a) as a_cardinality, finalizeAggregation(b) as b_cardinality
from
(select arrayReduce('uniqThetaState',[1,2]) as a, arrayReduce('uniqThetaState',[2,3,4]) as b );
```

```text
┌─a_union_b─┬─a_cardinality─┬─b_cardinality─┐
│         4 │             2 │             3 │
└───────────┴───────────────┴───────────────┘
```

## uniqThetaIntersect {#uniqthetaintersect}

Two uniqThetaSketch objects to do intersect calculation(set operation ∩), the result is a new uniqThetaSketch.

```sql
uniqThetaIntersect(uniqThetaSketch,uniqThetaSketch)
```

**Arguments**

- `uniqThetaSketch` – uniqThetaSketch object.

**Example**

```sql
select finalizeAggregation(uniqThetaIntersect(a, b)) as a_intersect_b, finalizeAggregation(a) as a_cardinality, finalizeAggregation(b) as b_cardinality
from
(select arrayReduce('uniqThetaState',[1,2]) as a, arrayReduce('uniqThetaState',[2,3,4]) as b );
```

```text
┌─a_intersect_b─┬─a_cardinality─┬─b_cardinality─┐
│             1 │             2 │             3 │
└───────────────┴───────────────┴───────────────┘
```

## uniqThetaNot {#uniqthetanot}

Two uniqThetaSketch objects to do a_not_b calculation(set operation ×), the result is a new uniqThetaSketch.

```sql
uniqThetaNot(uniqThetaSketch,uniqThetaSketch)
```

**Arguments**

- `uniqThetaSketch` – uniqThetaSketch object.

**Example**

```sql
select finalizeAggregation(uniqThetaNot(a, b)) as a_not_b, finalizeAggregation(a) as a_cardinality, finalizeAggregation(b) as b_cardinality
from
(select arrayReduce('uniqThetaState',[2,3,4]) as a, arrayReduce('uniqThetaState',[1,2]) as b );
```

```text
┌─a_not_b─┬─a_cardinality─┬─b_cardinality─┐
│       2 │             3 │             2 │
└─────────┴───────────────┴───────────────┘
```

**See Also**

- [uniqThetaSketch](/sql-reference/aggregate-functions/reference/uniqthetasketch)
