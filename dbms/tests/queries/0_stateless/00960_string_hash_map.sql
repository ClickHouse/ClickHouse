select name, count() from (select arrayJoin(['alexey', 'amos', 'amos']) name) group by name settings short_string_optimization = 1;
