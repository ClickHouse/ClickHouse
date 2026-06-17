-- Minor change of values to ensure non-empty result
-- dropped "mc.note LIKE '%(France)%'"; added 'Dutch' to mi.info IN-list
SELECT MIN(t.title) AS typical_european_movie
FROM company_type AS ct, info_type AS it, movie_companies AS mc, movie_info AS mi, title AS t
WHERE (ct.kind = 'production companies') AND (mc.note LIKE '%(theatrical)%') AND (mi.info IN ('Dutch', 'Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Denish', 'Norwegian', 'German')) AND (t.production_year > 2005) AND (t.id = mi.movie_id) AND (t.id = mc.movie_id) AND (mc.movie_id = mi.movie_id) AND (ct.id = mc.company_type_id) AND (it.id = mi.info_type_id);
