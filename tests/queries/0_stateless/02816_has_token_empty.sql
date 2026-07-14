SELECT hasTokenCaseInsensitive('K(G', ''); -- { serverError BAD_ARGUMENTS }
SELECT hasTokenCaseInsensitive('Hello', ''); -- { serverError BAD_ARGUMENTS }
SELECT hasTokenCaseInsensitive('', ''); -- { serverError BAD_ARGUMENTS }
SELECT hasTokenCaseInsensitive('', 'Hello');
SELECT hasTokenCaseInsensitiveOrNull('Hello', '');
SELECT hasTokenCaseInsensitiveOrNull('', '');
SELECT hasToken('Hello', ''); -- { serverError BAD_ARGUMENTS }
SELECT hasToken('', 'Hello');
SELECT hasToken('', ''); -- { serverError BAD_ARGUMENTS }
SELECT hasTokenOrNull('', '');
SELECT hasTokenOrNull('Hello', '');
