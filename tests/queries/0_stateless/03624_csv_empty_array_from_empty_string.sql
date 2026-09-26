 SELECT * FROM format(CSV, 'c0 Array(Int)', '""'); -- {serverError CANNOT_READ_ARRAY_FROM_TEXT}
 SELECT * FROM format(CSV, 'c0 Variant(String, Array(Int))', '""');

