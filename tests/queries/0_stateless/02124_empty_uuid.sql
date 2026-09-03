SELECT
    arrayJoin([toUUID('00000000-0000-0000-0000-000000000000'), toUUID('992f6910-42b2-43cd-98bc-c812fbf9b683')]) AS x,
    empty(x) AS emp;

SELECT
    arrayJoin([toUUID('992f6910-42b2-43cd-98bc-c812fbf9b683'), toUUID('00000000-0000-0000-0000-000000000000')]) AS x,
    empty(x) AS emp;
