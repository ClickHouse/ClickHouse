SELECT match('', repeat('(', 100000)); -- { serverError CANNOT_COMPILE_REGEXP }
