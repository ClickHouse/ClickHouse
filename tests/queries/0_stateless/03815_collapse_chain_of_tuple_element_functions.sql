select '{"a.b.c" : 42}'::JSON.a.b.c;
select materialize('{"a.b.c" : 42}')::JSON.a.b.c;
select '{"a.b" : [{"c.d" : 42}]}'::JSON.a.b[1].c.d;
select tuple(tuple(tuple(42)))::Tuple(a Tuple(b Tuple(c UInt32))).a.b.c;
select tuple(tuple(tuple(42)))::Tuple(a Tuple(b Tuple(c UInt32))).1.b.c;
select tuple(tuple(tuple(42)))::Tuple(a Tuple(b Tuple(c UInt32))).a.1.c;
select tuple(tuple(tuple(42)))::Tuple(a Tuple(b Tuple(c UInt32))).a.b.1;

select tupleElement(tupleElement(tupleElement(tuple(tuple(tuple(43)))::Tuple(a Tuple(b Tuple(c UInt32))), 'a'), 'b'), 'd', 42);

select tuple(tuple(tuple('{"d.e" : [{"f.g" : 42}]}')))::Tuple(a Tuple(b Tuple(c JSON))).a.b.c.d.e[1].f.g;
select tuple(tuple(tuple('{"d.e" : [{"f.g" : 42}]}')))::Tuple(a Tuple(b Tuple(c JSON))).1.b.c.d.e[1].f.g;
select tuple(tuple(tuple('{"d.e" : [{"f.g" : 42}]}')))::Tuple(a Tuple(b Tuple(c JSON))).a.1.c.d.e[1].f.g;
select tuple(tuple(tuple('{"d.e" : [{"f.g" : 42}]}')))::Tuple(a Tuple(b Tuple(c JSON))).a.b.1.d.e[1].f.g;

select tuple(tuple(tuple('{"d.e" : [{"f.g" : 42}]}')))::Tuple(a Tuple(b Tuple(c JSON)))::Dynamic.a.b.c.d.e[1].f.g;
select tuple(tuple(tuple('{"d.e" : [{"f.g" : 42}]}')))::Tuple(a Tuple(b Tuple(c JSON)))::Dynamic.1.b.c.d.e[1].f.g;
select tuple(tuple(tuple('{"d.e" : [{"f.g" : 42}]}')))::Tuple(a Tuple(b Tuple(c JSON)))::Dynamic.a.1.c.d.e[1].f.g;
select tuple(tuple(tuple('{"d.e" : [{"f.g" : 42}]}')))::Tuple(a Tuple(b Tuple(c JSON)))::Dynamic.a.b.1.d.e[1].f.g;
