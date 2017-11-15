Functions for working with arrays
---------------------------------

empty
~~~~~
Returns 1 for an empty array, or 0 for a non-empty array.
The result type is UInt8.
The function also works for strings.

notEmpty
~~~~~~~~
Returns 0 for an empty array, or 1 for a non-empty array.
The result type is UInt8.
The function also works for strings.

length
~~~~~~
Returns the number of items in the array.
The result type is UInt64.
The function also works for strings.

emptyArrayUInt8, emptyArrayUInt16, emptyArrayUInt32, emptyArrayUInt64
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

emptyArrayInt8, emptyArrayInt16, emptyArrayInt32, emptyArrayInt64
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

emptyArrayFloat32, emptyArrayFloat64
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

emptyArrayDate, emptyArrayDateTime
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

emptyArrayString
~~~~~~~~~~~~~~~~
Accepts zero arguments and returns an empty array of the appropriate type.

emptyArrayToSingle
~~~~~~~~~~~~~~~~~~
Accepts an empty array as argument and returns an array of one element equal to the default value.

range(N)
~~~~~~~~
Returns an array of numbers from 0 to N-1.
Just in case, an exception is thrown if arrays with a total length of more than 100,000,000 elements are created in a data block.

array(x1, ...), operator [x1, ...]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Creates an array from the function arguments.
The arguments must be constants and have types that have the smallest common type. At least one argument must be passed, because otherwise it isn't clear which type of array to create. That is, you can't use this function to create an empty array (to do that, use the 'emptyArray*' function described above).
Returns an 'Array(T)' type result, where 'T' is the smallest common type out of the passed arguments.

arrayElement(arr, n), operator arr[n]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Get the element with the index 'n' from the array 'arr'.
'n' should be any integer type.
Indexes in an array begin from one.
Negative indexes are supported - in this case, it selects the corresponding element numbered from the end. For example, 'arr[-1]' is the last item in the array.

If the index goes beyond the array bounds, a default value is returned (0 for numbers, an empty string for strings, etc.).

has(arr, elem)
~~~~~~~~~~~~~~
Checks whether the 'arr' array has the 'elem' element.
Returns 0 if the the element is not in the array, or 1 if it is.

indexOf(arr, x)
~~~~~~~~~~~~~~~
Returns the index of the 'x' element (starting from 1) if it is in the array, or 0 if it is not.

countEqual(arr, x)
~~~~~~~~~~~~~~~~~~
Returns the number of elements in the array equal to 'x'. Equivalent to ``arrayCount(elem -> elem = x, arr)``.

arrayEnumerate(arr)
~~~~~~~~~~~~~~~~~~~
Returns the array ``[1, 2, 3, ..., length(arr)]``

This function is normally used together with ARRAY JOIN. It allows counting something just once for each array after applying ARRAY JOIN. Example:

.. code-block:: sql

  SELECT
      count() AS Reaches,
      countIf(num = 1) AS Hits
  FROM test.hits
  ARRAY JOIN
      GoalsReached,
      arrayEnumerate(GoalsReached) AS num
  WHERE CounterID = 160656
  LIMIT 10

.. code-block:: text

  ┌─Reaches─┬──Hits─┐
  │   95606 │ 31406 │
  └─────────┴───────┘

In this example, Reaches is the number of conversions (the strings received after applying ARRAY JOIN), and Hits is the number of pageviews (strings before ARRAY JOIN). In this particular case, you can get the same result in an easier way:

.. code-block:: sql

  SELECT
      sum(length(GoalsReached)) AS Reaches,
      count() AS Hits
  FROM test.hits
  WHERE (CounterID = 160656) AND notEmpty(GoalsReached)

.. code-block:: text

  ┌─Reaches─┬──Hits─┐
  │   95606 │ 31406 │
  └─────────┴───────┘

This function can also be used in higher-order functions. For example, you can use it to get array indexes for elements that match a condition.

arrayEnumerateUniq(arr, ...)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Returns an array the same size as the source array, indicating for each element what its position is among elements with the same value.
For example: ``arrayEnumerateUniq([10, 20, 10, 30]) = [1,  1,  2,  1]``.

This function is useful when using ARRAY JOIN and aggregation of array elements. Example:

.. code-block:: sql

  SELECT
      Goals.ID AS GoalID,
      sum(Sign) AS Reaches,
      sumIf(Sign, num = 1) AS Visits
  FROM test.visits
  ARRAY JOIN
      Goals,
      arrayEnumerateUniq(Goals.ID) AS num
  WHERE CounterID = 160656
  GROUP BY GoalID
  ORDER BY Reaches DESC
  LIMIT 10

.. code-block:: text

  ┌──GoalID─┬─Reaches─┬─Visits─┐
  │   53225 │    3214 │   1097 │
  │ 2825062 │    3188 │   1097 │
  │   56600 │    2803 │    488 │
  │ 1989037 │    2401 │    365 │
  │ 2830064 │    2396 │    910 │
  │ 1113562 │    2372 │    373 │
  │ 3270895 │    2262 │    812 │
  │ 1084657 │    2262 │    345 │
  │   56599 │    2260 │    799 │
  │ 3271094 │    2256 │    812 │
  └─────────┴─────────┴────────┘

In this example, each goal ID has a calculation of the number of conversions (each element in the Goals nested data structure is a goal that was reached, which we refer to as a conversion) and the number of sessions.
Without ARRAY JOIN, we would have counted the number of sessions as ``sum(Sign)``. But in this particular case, the rows were multiplied by the nested Goals structure, so in order to count each session one time after this,
we apply a condition to the value of the ``arrayEnumerateUniq(Goals.ID)`` function.

The arrayEnumerateUniq function can take multiple arrays of the same size as arguments. In this case, uniqueness is considered for tuples of elements in the same positions in all the arrays.

.. code-block:: sql

  SELECT arrayEnumerateUniq([1, 1, 1, 2, 2, 2], [1, 1, 2, 1, 1, 2]) AS res

.. code-block:: text

  ┌─res───────────┐
  │ [1,2,1,1,2,1] │
  └───────────────┘

This is necessary when using ARRAY JOIN with a nested data structure and further aggregation across multiple elements in this structure.

arrayUniq(arr, ...)
~~~~~~~~~~~~~~~~~~~
If a single array is passed, returns a number of unique elements in that array.
If multiple arrays of the same size are passed as arguments to the function, returns a number of unique tuples of elements in the same positions in all the arrays.

If you need an array of the unique elements, you can use ``arrayReduce('groupUniqArray', arr)``.

arrayJoin(arr)
~~~~~~~~~~~~~~
A special function. See the section "arrayJoin function".
