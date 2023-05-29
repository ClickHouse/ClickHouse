CREATE TABLE frequent_phrases (
    col_phrase String,
    col_count int

) ENGINE = Memory;


INSERT INTO frequent_phrases VALUES ('Город под подошвой', 1);
INSERT INTO frequent_phrases VALUES ('Мы ждем перемен', 10);
INSERT INTO frequent_phrases VALUES ('Светофоры госпошлины сборы и таможни', 100);
INSERT INTO frequent_phrases VALUES ('Вместо тепла', 100);
INSERT INTO frequent_phrases VALUES ('Пусть лучше', 1000);

SELECT 
SUM(col_count) as total_quotes, 
author
FROM frequent_phrases
GROUP BY ngramClassifyUTF8('songs-ru', col_phrase) as author

