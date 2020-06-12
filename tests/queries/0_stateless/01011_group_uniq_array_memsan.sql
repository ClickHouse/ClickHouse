select groupUniqArray(v) from values('id int, v Array(int)', (1, [2]), (1, [])) group by id;
