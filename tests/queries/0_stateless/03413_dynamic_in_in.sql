select 42::Dynamic in 42; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select materialize(42)::Dynamic in 42; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select [42::Dynamic] in [42]; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select [materialize(42)::Dynamic] in [42]; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select tuple(map(42, 42::Dynamic)) in tuple(map(42, 42)); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select tuple(map(42, materialize(42)::Dynamic)) in tuple(map(42, 42)); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

select '{}'::JSON in '{}'; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select materialize('{}'::JSON)::Dynamic in '{}'; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select ['{}'::JSON] in ['{}']; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select [materialize('{}')::JSON] in ['{}']; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select tuple(map(42, '{}'::JSON)) in tuple(map(42, '{}')); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select tuple(map(42, materialize('{}')::JSON)) in tuple(map(42, '{}')); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}


