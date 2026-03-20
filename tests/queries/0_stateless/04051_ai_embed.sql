-- Tags: no-fasttest, no-parallel

-- Test that AI_EMBED functions are gated by the experimental setting
SELECT AI_EMBED('conn', 'model', 'text'); -- { serverError SUPPORT_IS_DISABLED }
SELECT AI_EMBED_OR_NULL('conn', 'model', 'text'); -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_ai_functions = 1;

-- Test that constant arguments are enforced
SELECT AI_EMBED(number, 'model', 'text') FROM numbers(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT AI_EMBED('conn', number, 'text') FROM numbers(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Test that wrong number of arguments fails
SELECT AI_EMBED('text'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT AI_EMBED('conn', 'model', 'text', 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Test that non-existent named collection fails
SELECT AI_EMBED('nonexistent_collection', 'model', 'hello world'); -- { serverError NAMED_COLLECTION_DOESNT_EXIST }

-- Test return type inference
SELECT toTypeName(AI_EMBED('http://localhost:1', 'model', 'text')) FORMAT Null; -- { serverError * }
