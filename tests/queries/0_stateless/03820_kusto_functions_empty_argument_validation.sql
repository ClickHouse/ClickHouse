
SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

print bin(, 1.5); -- { clientError SYNTAX_ERROR }
print bin_at(, 2.5, 7); -- { clientError SYNTAX_ERROR }
print iif(, 1, 2); -- { clientError SYNTAX_ERROR }
print indexof('abc', , 0); -- { clientError SYNTAX_ERROR }
print extract("User: ([^,]+)", , "User: James, Email: James@example.com, Age: 29"); -- { clientError SYNTAX_ERROR }
