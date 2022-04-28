
def repl(q):
	q = q.replace("??", "xx");
	q = q.replace("\\", "\\\\")
	q = q.replace("\n", "");
	q = q.replace("\\\\n", "")
	q = q.replace("\"", "\\\"")
	return q

def gen_queries(queries):
	res = "std::vector<std::string> queries = {"
	for i, x in enumerate(queries):
		if (i != 0):
			res += ", "
		res += "\"" + repl(x) + "\""
	res += "};"
	return res

with open('statements_ascii.txt', 'r') as inp:
	queries = inp.read().split('$$');
	# queries = ["\\\""]
	res = gen_queries(queries)

	print(res)
