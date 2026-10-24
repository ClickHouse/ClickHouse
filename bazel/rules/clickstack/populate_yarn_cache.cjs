const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const cp = require('node:child_process');

const [pkg, nativeChecksums, repo] = process.argv.slice(2).map(p => path.resolve(p));
const src = path.join(repo, 'staging');
const cache = path.join(repo, 'cache');
const home = path.join(repo, 'home');
try {
  fs.cpSync(path.dirname(pkg), src, {recursive: true, dereference: true});
  fs.mkdirSync(cache, {recursive: true});
  fs.mkdirSync(home, {recursive: true});
  // The ClickStack export runs on the Linux x64 Node toolchain for either C++
  // target CPU. Fetch that architecture's native Yarn packages on every host.
  const rc = path.join(src, '.yarnrc.yml');
  if (fs.readFileSync(rc, 'utf8').includes('supportedArchitectures:')) {
    throw new Error('HyperDX specifies supportedArchitectures; review cache selection');
  }
  fs.appendFileSync(rc, '\nsupportedArchitectures:\n  os: [linux]\n  cpu: [x64]\n  libc: [glibc]\n');
  const env = {
    HOME: home, TMPDIR: home, PATH: path.dirname(process.execPath) + ':/usr/bin:/bin',
    CI: '1', LANG: 'C.UTF-8', TZ: 'UTC',
    YARN_ENABLE_GLOBAL_CACHE: 'false', YARN_ENABLE_MIRROR: 'false',
    YARN_ENABLE_HARDENED_MODE: 'false', YARN_CACHE_FOLDER: cache,
    YARN_NPM_REGISTRY_SERVER: 'https://registry.npmjs.org',
    PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD: '1',
  };
  const cli = path.join(src, '.yarn/releases/yarn-4.13.0.cjs');
  const proc = cp.spawnSync(process.execPath, [cli, 'install', '--immutable', '--mode=skip-build'],
    {cwd: src, env, stdio: 'inherit'});
  if (proc.error) throw proc.error;
  if (proc.status !== 0) throw new Error('yarn install --immutable failed: ' + proc.status);
  const supplementary = JSON.parse(fs.readFileSync(nativeChecksums, 'utf8'));
  const manifest = {};
  for (const name of fs.readdirSync(cache).sort()) {
    if (name === '.gitignore') continue;
    if (!name.endsWith('.zip')) throw new Error('Unexpected Yarn cache entry: ' + name);
    const bytes = fs.readFileSync(path.join(cache, name));
    manifest[name] = {sha256: crypto.createHash('sha256').update(bytes).digest('hex'), size: bytes.length};
  }
  for (const [name, record] of Object.entries(supplementary)) {
    if (!manifest[name] || record.sha256 !== manifest[name].sha256 || record.size !== manifest[name].size) {
      throw new Error('Checksum missing from yarn.lock and no matching native pin for ' + name);
    }
  }
  fs.writeFileSync(path.join(repo, 'manifest.json'), JSON.stringify(manifest) + '\n');
} finally {
  fs.rmSync(src, {recursive: true, force: true});
  fs.rmSync(home, {recursive: true, force: true});
}
