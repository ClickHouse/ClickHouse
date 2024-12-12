---
slug: /en/sql-reference/aggregate-functions/reference/grouparraysorted
sidebar_position: 146
---

 # groupArraySorted {#groupArraySorted}

 Returns an array with the first N items in ascending order.

 ``` sql
 groupArraySorted(N)(column)
 ```

 **Arguments**

 -   `N` – The number of elements to return.

 -   `column` – The value (Integer, String, Float and other Generic types).

 **Example**

 Gets the first 10 numbers:

 ``` sql
 SELECT groupArraySorted(10)(number) FROM numbers(100)
 ```

 ``` text
 ┌─groupArraySorted(10)(number)─┐
 │ [0,1,2,3,4,5,6,7,8,9]        │
 └──────────────────────────────┘
 ```


 Gets all the String implementations of all numbers in column:

 ``` sql
SELECT groupArraySorted(5)(str) FROM (SELECT toString(number) as str FROM numbers(5));

 ```

 ``` text
┌─groupArraySorted(5)(str)─┐
│ ['0','1','2','3','4']    │
└──────────────────────────┘
 ```