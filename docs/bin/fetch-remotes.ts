// Fetches docs owned by other repositories into their mount directories so the
// primary collection builds them at their current URLs. Sources, in order:
//   1. a local checkout named by the remote's `localPathEnv` variable,
//   2. a GitHub tarball of `repo@ref` (needs GH_TOKEN / GITHUB_TOKEN for private repos).
// Without either, a private remote is skipped (fork previews) and reported.
// Usage: node bin/fetch-remotes.ts
import fs from "node:fs";
import path from "node:path";
import { execFileSync } from "node:child_process";

interface Remote { name: string; repo: string; ref: string; path: string; mount: string; private?: boolean; localPathEnv?: string }
const root = process.cwd();
const manifest = JSON.parse(fs.readFileSync(path.join(root, "remotes.json"), "utf8")) as { remotes: Remote[] };
const stateDir = path.join(root, ".remote");
fs.mkdirSync(stateDir, { recursive: true });

const KEEP = new Set(["images"]); // committed assets inside the mount directory

function cleanMount(mount: string) {
  if (!fs.existsSync(mount)) { fs.mkdirSync(mount, { recursive: true }); return; }
  for (const e of fs.readdirSync(mount)) if (!KEEP.has(e)) fs.rmSync(path.join(mount, e), { recursive: true, force: true });
}

function copyTree(src: string, dst: string) {
  let n = 0;
  for (const e of fs.readdirSync(src, { withFileTypes: true })) {
    if (e.name === ".git" || e.name === "node_modules" || e.name === ".idea") continue;
    const s = path.join(src, e.name), d = path.join(dst, e.name);
    if (e.isDirectory()) { fs.mkdirSync(d, { recursive: true }); n += copyTree(s, d); }
    else if (/\.(mdx?|json|png|jpe?g|gif|svg|webp)$/i.test(e.name)) { fs.copyFileSync(s, d); n++; }
  }
  return n;
}

for (const r of manifest.remotes) {
  const mount = path.join(root, r.mount);
  const local = r.localPathEnv ? process.env[r.localPathEnv] : undefined;
  const token = process.env.GH_TOKEN ?? process.env.GITHUB_TOKEN;
  let source: string | null = null;
  let commit = "unknown";
  if (local && fs.existsSync(local)) {
    source = path.join(local, r.path);
    try { commit = execFileSync("git", ["-C", local, "rev-parse", "HEAD"], { encoding: "utf8" }).trim(); } catch { /* not a checkout */ }
  } else if (token || !r.private) {
    const tmp = fs.mkdtempSync(path.join(stateDir, `${r.name}-`));
    const tar = path.join(tmp, "src.tgz");
    const headers = token ? ["-H", `Authorization: Bearer ${token}`] : [];
    execFileSync("curl", ["-sSfL", ...headers, "-H", "Accept: application/vnd.github+json", `https://api.github.com/repos/${r.repo}/tarball/${r.ref}`, "-o", tar], { stdio: "inherit" });
    execFileSync("tar", ["-xzf", tar, "-C", tmp]);
    const extracted = fs.readdirSync(tmp).find((d) => d !== "src.tgz")!;
    source = path.join(tmp, extracted, r.path);
    commit = extracted.split("-").pop() ?? "unknown";
  }
  if (!source) {
    console.log(`fetch-remotes: ${r.name} skipped (private, no token and no ${r.localPathEnv})`);
    fs.writeFileSync(path.join(stateDir, `${r.name}.json`), JSON.stringify({ name: r.name, skipped: true }, null, 2));
    continue;
  }
  cleanMount(mount);
  const files = copyTree(source, mount);
  fs.writeFileSync(path.join(stateDir, `${r.name}.json`), JSON.stringify({ name: r.name, repo: r.repo, ref: r.ref, commit, fetchedAt: new Date().toISOString(), files }, null, 2));
  console.log(`fetch-remotes: ${r.name} <- ${local ? local : `${r.repo}@${r.ref}`} (${commit.slice(0, 12)}): ${files} files into ${r.mount}`);
}
