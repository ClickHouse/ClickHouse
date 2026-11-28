# Validation Decided by How an Object Is Loaded

TRIGGER
The diff adds, moves, relaxes, or re-gates a check that rejects part of an object definition (a feature gate, a suspicious-value or sanity check, a setting constraint, argument or structure validation, a security restriction), for any object whose definition is stored and replayed later: tables, views, dictionaries, databases, and the parts declared inside them.

TWO QUESTIONS, ONE SIGNAL
- A definition reaches the server either as input the user supplies now, or as a replay of a definition stored earlier. Validation must reject bad input, and must not make a stored object unloadable. Whether to validate is therefore a question about the provenance of the definition.
- The loading strictness level and the `attach` flags of the query answer a different question: whether the state of the object (data, paths, coordination nodes) is expected to exist already. The two do not line up. Some statements that run as an attach carry a complete definition typed by the user, and some replays run under a level that sorts below attach. So a comparison on the level, an `attach` flag, or a bool derived from either, used to decide whether to validate, admits user input on one path or refuses a stored object on another.

THE INVARIANT
- Every check that rejects definition content is decided by provenance, through the shared predicate (`isFreshDefinition` in `src/Databases/LoadingStrictnessLevel.h`, or whatever replaces it), not by a local re-derivation.
- The replay exemption covers every replay path and only those paths.
- A check that exists because the stored state itself is unsafe to load keeps running on replay.

WHAT TO VERIFY
1) **Source of the condition**
   - Trace the condition that guards the check back to where it is computed. A parameter or member named after attaching, loading, sanity, or skipping usually encodes the level. Follow it through constructors and helpers that receive only the level and cannot see whether the definition is fresh.
2) **Every carrier**
   - List every path that carries a fresh definition and confirm each one runs the check: the create statement, attach statements that state the full definition, create-like forms that copy or derive a definition from another object, and paths that execute the user's original statement on another replica or server.
   - Then list every replay path and confirm each one is exempt.
3) **Shared checks**
   - When the check is shared with other operations (alter, reading a stored definition back), confirm that the flag every caller passes means provenance, not a loading level.
4) **Siblings**
   - When the diff fixes one such check, fan out: other checks in the same function and file, and other consumers of the same derived flag, usually share the defect.

TESTS
- A new or re-gated check needs a negative test through each fresh-definition path, not only through create.
- When the check is new for definitions that may already be stored, it also needs a test that a stored object violating it still loads.

SEVERITY
- A security restriction skipped on a fresh-definition path is a **Blocker**.
- A feature gate or a sanity check skipped on such a path is a **Major**.
- A check that runs on replay and can refuse an already stored object is a **Major**, because the object can no longer be attached, dropped, or loaded at startup.
