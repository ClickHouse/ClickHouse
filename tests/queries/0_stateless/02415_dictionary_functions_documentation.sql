WITH dictionary_functions AS (
    SELECT
        name,
        introduced_in,
        syntax,
        arguments,
        returned_value,
        examples
    FROM system.functions WHERE categories='Dictionary' ORDER BY name ASC
)
SELECT
    format(
            '{}{}{}{}{}{}',
            '## ' || name || ' ' || printf('{#%s}', name) || '\n\n',
            'Introduced in: v'||introduced_in||'\n\n',
            '**Syntax**\n\n'||printf('```sql\n%s\n```', syntax)||'\n\n',
            '**Arguments**\n\n'||arguments||'\n',
            '**Returned value**\n\n'||trim(returned_value)||'\n\n',
            '**Examples**\n\n'||examples||'\n'
    )
FROM dictionary_functions
FORMAT LineAsString
