const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const cp = require('node:child_process');
const os = require('node:os');
const {createRequire} = require('node:module');

const args = process.argv.slice(2);
if (args.length !== 9 || process.platform !== 'linux' || process.arch !== 'x64') {
  throw new Error('Expected package.json, Yarn manifest, font manifest, out, release gitignore, four Unicode coverage inputs');
}
const [pkg, yarnFile, fontFile, out, gitignore] = args.slice(0, 5).map(p => path.resolve(p));
const coverage = {};
for (const arg of args.slice(5)) {
  const cut = arg.indexOf('=');
  if (cut === -1) throw new Error('Invalid Fontsource coverage argument: ' + arg);
  coverage[arg.slice(0, cut)] = JSON.parse(fs.readFileSync(path.resolve(arg.slice(cut + 1)), 'utf8'));
}
if (!out) {
  throw new Error('Expected package.json, Yarn manifest, font manifest, out; Linux x86_64 required');
}
const sha256 = (p) => crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex');
function declaredFile(root, filename, expected) {
  const p = path.resolve(root, filename);
  if (!p.startsWith(root + path.sep) || !/^[0-9a-f]{64}$/.test(expected) || sha256(p) !== expected) {
    throw new Error('Unpinned or mismatched input ' + filename);
  }
  return p;
}
const yarn = JSON.parse(fs.readFileSync(yarnFile));
const fonts = JSON.parse(fs.readFileSync(fontFile));
if (!Object.keys(yarn).length || !fonts.length) throw new Error('Empty offline input manifest');
const dest = path.resolve(out);
const temporary = fs.mkdtempSync(path.join(os.tmpdir(), 'hyperdx-source-'));
try {
  const src = path.join(temporary, 'source');
  fs.cpSync(path.dirname(pkg), src, {recursive: true, dereference: true});
  const cache = path.join(temporary, 'cache');
  const home = path.join(temporary, 'home');
  fs.mkdirSync(cache);
  fs.mkdirSync(home);
  for (const [name, record] of Object.entries(yarn)) {
    // Yarn adds its own .gitignore alongside cache archives.
    if (name === '.gitignore') continue;
    const input = declaredFile(path.dirname(yarnFile), 'cache/' + name, record.sha256);
    const target = path.resolve(cache, name);
    if (!target.startsWith(cache + path.sep) || !name.endsWith('.zip')) throw new Error('Unexpected Yarn cache file ' + name);
    fs.copyFileSync(input, target);
  }
  const cssByUrl = {};
  for (const entry of fonts) {
    if (!entry.url.startsWith('https://fonts.googleapis.com/')) throw new Error('Unknown CSS URL ' + entry.url);
    let css = fs.readFileSync(declaredFile(path.dirname(fontFile), entry.css, entry.sha256), 'utf8');
    for (const font of entry.fonts) {
      if (!font.url.startsWith('https://fonts.gstatic.com/')) throw new Error('Unknown WOFF URL ' + font.url);
      const filename = declaredFile(path.dirname(fontFile), font.file, font.sha256);
      if (!css.includes(font.url)) throw new Error('Unused declared WOFF input ' + font.file);
      css = css.replaceAll(font.url, filename);
    }
    const family = entry.url.split('family=')[1].split(':')[0];
    if (!coverage[family]) throw new Error('No Fontsource metadata for ' + family);
    let count = 0;
    css = css.replace(/(\/\* ([^*]+?) \*\/\s*@font-face\s*\{[^}]*?unicode-range:\s*)[^;]+;/g,
      (_, prefix, subset) => {
        if (!coverage[family][subset]) throw new Error('No Fontsource subset ' + subset + ' for ' + family);
        count++;
        return prefix + coverage[family][subset].split(',').join(', ') + ';';
      });
    if (!count) throw new Error('No font faces in pinned Google CSS ' + entry.url);
    if (/url\(\s*["']?https?:/.test(css)) throw new Error('CSS has undeclared remote font URLs: ' + entry.url);
    cssByUrl[entry.url] = css;
  }
  const mock = path.join(temporary, 'google-fonts.cjs');
  fs.writeFileSync(mock, 'module.exports = ' + JSON.stringify(cssByUrl) + ';\n');
  // Yarn and Next write to the staged source tree. No host npm config,
  // credentials, cached build output, or ClickStack export is visible here.
  const env = {
    // process.execPath is rules_js's Node launcher. Retain its declared
    // toolchain paths and FS patch settings while excluding host settings.
    ...Object.fromEntries(Object.entries(process.env).filter(([key]) => key.startsWith('JS_BINARY__'))),
    HOME: home, TMPDIR: temporary, PATH: path.dirname(process.execPath) + ':/usr/bin:/bin',
    CI: '1', LANG: 'C.UTF-8', TZ: 'UTC',
    YARN_ENABLE_NETWORK: 'false', YARN_ENABLE_GLOBAL_CACHE: 'false',
    YARN_ENABLE_MIRROR: 'false', YARN_ENABLE_HARDENED_MODE: 'false',
    YARN_CACHE_FOLDER: cache,
    NEXT_FONT_GOOGLE_MOCKED_RESPONSES: mock, NEXT_TELEMETRY_DISABLED: '1',
    NX_DAEMON: 'false', NX_NO_CLOUD: 'true',
    NX_SKIP_NX_CACHE: 'true', NX_SKIP_REMOTE_CACHE: 'true',
  };
  const cli = path.join(src, '.yarn/releases/yarn-4.13.0.cjs');
  for (const args of [['install', '--immutable', '--immutable-cache', '--mode=skip-build'], ['build:clickhouse']]) {
    const proc = cp.spawnSync(process.execPath, [cli, ...args], {cwd: src, env, stdio: 'inherit'});
    if (proc.error) throw proc.error;
    if (proc.status !== 0) throw new Error('yarn ' + args.join(' ') + ' failed: ' + proc.status);
  }
  // "make sync" in clickhouse-clickstack copies this out/ and git adds it.
  // Apply the pinned .gitignore using the parser in HyperDX's locked Yarn tree.
  const sourceRequire = createRequire(path.join(src, 'package.json'));
  const ignored = sourceRequire('ignore')().add(fs.readFileSync(gitignore, 'utf8'));
  const built = path.join(src, 'packages/app/out');
  fs.cpSync(built, dest, {
    recursive: true, dereference: true,
    filter: name => {
      const relative = path.relative(built, name);
      return !relative || !ignored.ignores(fs.statSync(name).isDirectory() ? relative + '/' : relative);
    },
  });
} finally {
  fs.rmSync(temporary, {recursive: true, force: true});
}
