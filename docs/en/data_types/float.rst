Float32, Float64
----------------

Floating-point numbers are just like 'float' and 'double' in the C language.
In contrast to standard SQL, floating-point numbers support 'inf', '-inf', and even 'nan's.
See the notes on sorting nans in "ORDER BY clause".
We do not recommend storing floating-point numbers in tables.
