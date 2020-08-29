SELECT any(if(if(x, 1, 2) AS a_, a_, 0)) FROM (SELECT 1 AS x);
