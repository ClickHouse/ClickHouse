SET enable_analyzer = 1;
SET enable_named_columns_in_function_tuple=1;

SELECT JSONExtract('{"hello":[{"world":"wtf"}]}', 'Tuple(hello Array(Tuple(world String)))') AS x,
    x.hello, x.hello[1].world;

SELECT JSONExtract('{"hello":[{" wow ":"wtf"}]}', 'Tuple(hello Array(Tuple(` wow ` String)))') AS x,
    x.hello, x.hello[1].` wow `;

SELECT JSONExtract('{"hello":[{" wow ":"wtf"}]}', 'Tuple(hello Array(Tuple(` wow ` String)))') AS x,
    x.hello, x.hello[1].`wow`; -- { serverError BAD_ARGUMENTS }

SELECT ('Hello' AS world,).world;
SELECT ('Hello' AS world,) AS t, t.world, (t).world, identity(t).world;
