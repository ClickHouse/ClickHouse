SELECT count() FROM (SELECT sum(materialize(1)), sum(materialize(2)))
