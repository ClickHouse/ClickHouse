-- Tags: no-fasttest

SELECT number FROM numbers(11) ORDER BY arrayJoin(['а', 'я', '\0�', '', 'Я', '']) ASC, toString(number) ASC, 'y' ASC COLLATE 'el';
