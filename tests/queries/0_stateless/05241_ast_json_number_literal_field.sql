-- A numeric literal carried as a `Field` dump payload in a JSON AST.

SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"Number","value":"Number_-170141183460469231731687303715884105728"}}]}}]}}');
SELECT formatQueryFromJSON('{"type":"Literal","value":{"field_type":"Array","value":[{"field_type":"Number","value":"Number_5"},{"field_type":"Number","value":"Number_340282366920938463463374607431768211455"}]}}');
