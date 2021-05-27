SourceID = int
FuncIndex = int
Line = int

FuncName = str
FuncInfo = tuple[FuncName, Line]
FuncsInstrumented = dict[FuncIndex, FuncInfo]
LinesInstrumented = list[Line]

RelativePath = str
SourceFile = tuple[RelativePath, FuncsInstrumented, LinesInstrumented]

FuncsHit = list[FuncIndex]
LinesHit = list[Line]
Test = dict[SourceID, tuple[FuncsHit, LinesHit]]
