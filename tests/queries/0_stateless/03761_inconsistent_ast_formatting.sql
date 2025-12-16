select (-(42))[3]; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select(-('a')).1; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
