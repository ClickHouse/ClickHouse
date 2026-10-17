// The file tree's model: rows and what the filter leaves visible. Pure data —
// sidebar.mjs turns a row into DOM and never recomputes what is in here.

/// One flat row list, each row carrying its depth and the directories above it,
/// so filtering and collapsing are a pass over an array rather than a rebuild.
/// Rows are `{ kind: 'all' | 'dir' | 'file', label, depth, ancestors, … }`.
export function buildRows(files) {
  const rows = [{ kind: 'all', label: 'All files', depth: 0, ancestors: [], count: files.length }];
  emit(treeOf(files), 0, '', [], rows);
  return rows;
}

function treeOf(files) {
  const root = { dirs: new Map(), files: [] };
  for (const file of files) {
    const parts = file.path.split('/');
    let node = root;
    for (const dir of parts.slice(0, -1)) {
      if (!node.dirs.has(dir)) node.dirs.set(dir, { dirs: new Map(), files: [] });
      node = node.dirs.get(dir);
    }
    node.files.push(file);
  }
  return root;
}

/// Fold chains of single-child directories into one row (`src/Processors/QueryPlan`),
/// which is what keeps a deep tree readable.
function foldChain(name, node) {
  while (node.files.length === 0 && node.dirs.size === 1) {
    const [childName, child] = [...node.dirs][0];
    name = `${name}/${childName}`;
    node = child;
  }
  return [name, node];
}

function countFiles(node) {
  let n = node.files.length;
  for (const child of node.dirs.values()) n += countFiles(child);
  return n;
}

function emit(node, depth, prefix, ancestors, rows) {
  for (const [rawName, rawChild] of node.dirs) {
    const [label, child] = foldChain(rawName, rawChild);
    const dirKey = `${prefix}${label}/`;
    rows.push({ kind: 'dir', label, depth, ancestors, dirKey, count: countFiles(child) });
    emit(child, depth + 1, dirKey, [...ancestors, dirKey], rows);
  }
  for (const file of node.files) {
    rows.push({
      kind: 'file',
      label: file.path.split('/').pop(),
      depth,
      ancestors,
      path: file.path,
      file,
    });
  }
}

/// Which rows survive the path filter and the collapsed directories, aligned
/// with `rows`. A directory stays visible while any file under it matches, so
/// the path to a match is never hidden by its own name failing the filter.
export function visibility(rows, { query = '', collapsed = new Set() } = {}) {
  const needle = query.trim().toLowerCase();
  const matches = (row) =>
    needle === '' || (row.path ?? row.dirKey ?? '').toLowerCase().includes(needle);

  const keptDirs = new Set();
  for (const row of rows) {
    if (row.kind === 'file' && matches(row)) for (const dir of row.ancestors) keptDirs.add(dir);
  }
  return rows.map((row) => {
    if (row.kind === 'all') return true;
    if (row.ancestors.some((dir) => collapsed.has(dir))) return false;
    return row.kind === 'file' ? matches(row) : keptDirs.has(row.dirKey) || matches(row);
  });
}

export function visiblePaths(rows, visible) {
  return rows.filter((row, i) => row.kind === 'file' && visible[i]).map((row) => row.path);
}
