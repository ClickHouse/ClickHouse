---
description: 'Aggregates arrays into a larger array of those arrays.'
keywords: ['groupArrayArray', 'array_concat_agg']
sidebar_position: 111
slug: /sql-reference/aggregate-functions/reference/grouparrayarray
title: 'groupArrayArray'
---

# groupArrayArray

Aggregates arrays into a larger array of those arrays.
Combines the [`groupArray`](/sql-reference/aggregate-functions/reference/grouparray) function with the [Array](/sql-reference/aggregate-functions/combinators#-array) combinator.

Alias: `array_concat_agg`

**Example**

We have data which captures user browsing sessions. Each session records the sequence of pages a specific user visited. 
We can use the `groupArrayArray` function to analyze the patterns of page visits for each user.

```sql title="Setup"
CREATE TABLE website_visits (
    user_id UInt32,
    session_id UInt32,
    page_visits Array(String)
) ENGINE = Memory;

INSERT INTO website_visits VALUES
(101, 1, ['homepage', 'products', 'checkout']),
(101, 2, ['search', 'product_details', 'contact']),
(102, 1, ['homepage', 'about_us']),
(101, 3, ['blog', 'homepage']),
(102, 2, ['products', 'product_details', 'add_to_cart', 'checkout']);
```

```sql title="Query"
SELECT
    user_id,
    groupArrayArray(page_visits) AS user_session_page_sequences
FROM website_visits
GROUP BY user_id;
```

```sql title="Response"
   ┌─user_id─┬─user_session_page_sequences───────────────────────────────────────────────────────────────┐
1. │     101 │ ['homepage','products','checkout','search','product_details','contact','blog','homepage'] │
2. │     102 │ ['homepage','about_us','products','product_details','add_to_cart','checkout']             │
   └─────────┴───────────────────────────────────────────────────────────────────────────────────────────┘
```
