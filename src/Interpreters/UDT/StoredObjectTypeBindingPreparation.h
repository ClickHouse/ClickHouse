

/// Resolves an exact literal schema slot owned by `CREATE TABLE ... AS
/// table_function()` and rewrites it to canonical physical column types before
/// the table function is instantiated or its AST can be persisted.  This is a
/// transient-resolution operation: inferred destination columns remain
/// physical and no descriptor, sidecar, dependency edge, or binding handoff is
/// produced.  The preparation decision and a fresh structural classification
/// must designate the dedicated physicalization route.
void physicalizeInferredTableFunctionSchema(
    ASTCreateQuery & create,
    const StoredObjectCreateQueryClassification & classification,
    const StoredObjectCreatePreparationDecision & decision,
    const ContextPtr & context);

/// Converts the runtime-only stored-expression annotations of one private
/// View/MV SELECT clone into ordinary physical CAST targets before its owning
/// logical binding is erased. The traversal is closed and bounded, requires
/// tagged CAST ordinals to be contiguous in owner-walk order, and applies no
/// AST mutation until every tagged endpoint has been validated.
void physicalizeViewStoredSelectRuntimeAnnotations(const ASTPtr & stored_select);