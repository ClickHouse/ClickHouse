-- Aggregate function documentation generator
-- Usage: Pass the function name as a parameter, e.g.:
-- clickhouse --param_function_name='avg' --queries-file generate-aggregate-functions.sql

WITH function_docs AS (
    SELECT
        name,
        description,
        introduced_in,
        syntax,
        arguments,
        returned_value,
        examples,
        parameters  -- AGGREGATE-SPECIFIC FIELD
    FROM system.functions
    WHERE name = {function_name:String}
    AND is_aggregate = 1
    AND alias_to = ''
    LIMIT 1
),
function_aliases AS (
    SELECT
        alias_to AS function_name,
        groupArray(name) AS aliases
    FROM system.functions
    WHERE alias_to != ''
    AND is_aggregate = 1
    AND alias_to = {function_name:String}
    GROUP BY alias_to
)
SELECT
    format(
        '{}{}{}{}{}{}{}{}{}',
        '## ' || fd.name || ' ' || printf('{#%s}', fd.name) || '\n\n',
        'Introduced in: v' || fd.introduced_in || '\n\n',
        fd.description || '\n\n',
        '**Syntax**\n\n' || printf('```sql\n%s\n```', fd.syntax) || '\n\n',
        if(fa.aliases IS NOT NULL AND length(fa.aliases) > 0,
           '**Aliases**: ' || arrayStringConcat(arrayMap(x -> '`' || x || '`', fa.aliases), ', ') || '\n\n',
           ''),
        if(empty(fd.parameters), '', '**Parameters**\n\n' || fd.parameters || '\n\n'),
        if(empty(fd.arguments), '**Arguments**\n\n- None.\n\n', '**Arguments**\n\n' || fd.arguments || '\n\n'),
        '**Returned value**\n\n' || trim(fd.returned_value) || '\n\n',
        '**Examples**\n\n' || fd.examples || '\n'
    )
FROM function_docs fd
LEFT JOIN function_aliases fa ON fd.name = fa.function_name
INTO OUTFILE 'temp-aggregate-function.md' TRUNCATE FORMAT LineAsString