-- Tags: no-fasttest
-- Tag no-fasttest: requires ai-sdk-cpp (USE_CLIENT_AI)

-- Verify the function exists and setting gate works
SELECT ai_generate_text('', 'test'); -- { serverError SUPPORT_IS_DISABLED }

-- Enable the experimental setting
SET allow_experimental_ai_functions = 1;

-- Verify argument count validation (2 or 3 arguments)
SELECT ai_generate_text('test'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT ai_generate_text('a', 'b', 'c', 'd'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Verify argument type validation
SELECT ai_generate_text(1, 'test'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT ai_generate_text('', 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT ai_generate_text('', 'test', 123); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
