-- Two `*` matchers whose column transformers differ in kind at the same position must report the
-- alias conflict rather than fail an internal type assertion.

SET enable_analyzer = 1;

-- All six ordered kind pairs reach IQueryTreeNode::isEqual through the duplicate-alias check.
WITH x -> * APPLY toString AS lambda, x -> * EXCEPT (a) AS lambda SELECT lambda(1); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
WITH x -> * EXCEPT (a) AS lambda, x -> * APPLY toString AS lambda SELECT lambda(1); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
WITH x -> * APPLY toString AS lambda, x -> * REPLACE (1 AS a) AS lambda SELECT lambda(1); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
WITH x -> * REPLACE (1 AS a) AS lambda, x -> * APPLY toString AS lambda SELECT lambda(1); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
WITH x -> * EXCEPT (a) AS lambda, x -> * REPLACE (1 AS a) AS lambda SELECT lambda(1); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
WITH x -> * REPLACE (1 AS a) AS lambda, x -> * EXCEPT (a) AS lambda SELECT lambda(1); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

-- Same kind, different contents: the per-kind comparison still reports them as different.
WITH x -> * EXCEPT (a) AS lambda, x -> * EXCEPT (b) AS lambda SELECT lambda(1); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

-- Same kind, same contents: the comparison still reports them as equal, so the alias is accepted.
WITH x -> * APPLY toString AS lambda, x -> * APPLY toString AS lambda SELECT lambda(1);
WITH x -> * EXCEPT (a) AS lambda, x -> * EXCEPT (a) AS lambda SELECT lambda(1);
WITH x -> * REPLACE (1 AS a) AS lambda, x -> * REPLACE (1 AS a) AS lambda SELECT lambda(1);
WITH x -> x + 1 AS lambda, x -> x + 1 AS lambda SELECT lambda(1);
