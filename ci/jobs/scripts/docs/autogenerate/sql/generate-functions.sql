-- Consolidated function documentation generator
-- Usage: Pass the category as a parameter, e.g.:
-- clickhouse --param_category='Arithmetic' --queries-file generate-functions.sql

WITH function_docs AS (
    SELECT
        name,
        description,
        introduced_in,
        syntax,
        arguments,
        returned_value,
        examples,
        deterministic
    FROM system.functions
    WHERE categories = {category:String}
    AND alias_to = ''
    AND is_aggregate = 0 -- aggregate functions are documented on their own reference pages
ORDER BY name ASC
    ),
function_aliases AS (
    SELECT
        alias_to AS function_name,
        groupArray(name) AS aliases
    FROM system.functions
    WHERE alias_to != ''
    AND alias_to IN (SELECT name FROM function_docs)
    GROUP BY alias_to
)
SELECT
    format(
        '{}{}{}{}{}{}{}{}{}',
        '## ' || fd.name || ' ' || printf('{#%s}', fd.name) || '\n\n',
        'Introduced in: v' || fd.introduced_in || '\n\n',
        fd.description || '\n\n',
        if(coalesce(fd.deterministic, 1) = 0, ':::note\nThis function is non-deterministic: it can return different results for the same arguments.\n:::\n\n', ''),
        '**Syntax**\n\n' || printf('```sql\n%s\n```', fd.syntax) || '\n\n',
        if(fa.aliases IS NOT NULL AND length(fa.aliases) > 0,
           '**Aliases**: ' || arrayStringConcat(arrayMap(x -> '`' || x || '`', fa.aliases), ', ') || '\n\n',
           ''),
        if(empty(fd.arguments), '**Arguments**\n\n- None.\n\n', '**Arguments**\n\n' || fd.arguments || '\n\n'),
        '**Returned value**\n\n' || trim(fd.returned_value) || '\n\n',
        '**Examples**\n\n' || fd.examples || '\n'
    )
FROM function_docs fd
LEFT JOIN function_aliases fa ON fd.name = fa.function_name
INTO OUTFILE 'temp-functions.md' TRUNCATE FORMAT LineAsString
