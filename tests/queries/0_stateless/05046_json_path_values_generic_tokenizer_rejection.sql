SELECT tokens('{}', 'jsonPathValues'); -- { serverError BAD_ARGUMENTS }
SELECT tokensForLikePattern('{}', 'jsonPathValues'); -- { serverError BAD_ARGUMENTS }
SELECT hasAnyTokens('{}', ['value'], 'jsonPathValues'); -- { serverError BAD_ARGUMENTS }
SELECT hasAllTokens('{}', ['value'], 'jsonPathValues'); -- { serverError BAD_ARGUMENTS }
