SELECT x, a FROM (SELECT arrayJoin(['Hello', 'Goodbye']) AS x, [1, 2, 3] AS arr) ARRAY JOIN; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT x, a FROM (SELECT arrayJoin(['Hello', 'Goodbye']) AS x, [1, 2, 3] AS arr) ARRAY JOIN arr AS a;
