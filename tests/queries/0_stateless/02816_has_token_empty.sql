SELECT hasTokenCaseInsensitive('K(G', ''); -- { serverError BAD_ARGUMENTS }
SELECT hasTokenCaseInsensitive('Hello', ''); -- { serverError BAD_ARGUMENTS }
SELECT hasTokenCaseInsensitive('', ''); -- { serverError BAD_ARGUMENTS }
SELECT hasTokenCaseInsensitive('', 'Hello');
SELECT hasToken('Hello', ''); -- { serverError BAD_ARGUMENTS }
SELECT hasToken('', 'Hello');
SELECT hasToken('', ''); -- { serverError BAD_ARGUMENTS }
