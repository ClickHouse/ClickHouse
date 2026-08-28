import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

import mdx from '@astrojs/mdx';
import { unified } from '@astrojs/markdown-remark';
import react from '@astrojs/react';
import { mintlify } from '@mintlify/astro';
import tailwindcss from '@tailwindcss/vite';
import { defineConfig } from 'astro/config';
import rehypeKatex from 'rehype-katex';
import remarkMath from 'remark-math';

import rehypeCodeBlocks from './src/markdown/code-blocks.mjs';
import rehypeCustomCodeHighlighter from './src/markdown/custom-code-highlighter.mjs';
import rehypeHeadingAnchors from './src/markdown/heading-anchors.mjs';
import rewriteGeneratedCodeBlocks from './src/markdown/rewrite-generated-code-blocks.mjs';
import versionSnapshotFiles from './scripts/lib/version-snapshot-files.mjs';

const snapshotsDirectory = fileURLToPath(new URL('./.snapshots', import.meta.url));
const generatedManifest = JSON.parse(
  readFileSync(new URL('./src/generated/data/manifest.json', import.meta.url), 'utf8'),
);
const referenceBasePath = generatedManifest.channel === 'latest'
  ? 'docs/reference'
  : `docs/reference/versions/${generatedManifest.channel}`;

export default defineConfig({
  site: 'https://clickhouse.com',
  output: 'static',
  prefetch: false,
  publicDir: './src/generated/public',
  trailingSlash: 'ignore',
  build: {
    assets: `${referenceBasePath}/_astro`,
  },
  markdown: {
    syntaxHighlight: false,
    processor: unified({
      remarkPlugins: [remarkMath],
      rehypePlugins: [
        rehypeKatex,
        rehypeCustomCodeHighlighter,
        rehypeCodeBlocks,
        rehypeHeadingAnchors,
      ],
    }),
  },
  integrations: [
    mintlify({ docsDir: './src/generated/mintlify' }),
    rewriteGeneratedCodeBlocks(),
    react(),
    mdx(),
  ],
  vite: {
    plugins: [versionSnapshotFiles({ snapshotsDirectory }), tailwindcss()],
    resolve: {
      alias: {
        '@components': fileURLToPath(new URL('./src/components', import.meta.url)),
        '@clickhouse-docs-components': fileURLToPath(new URL('./src/generated/shared/components', import.meta.url)),
        '@generated': fileURLToPath(new URL('./src/generated', import.meta.url)),
        '/snippets/components': fileURLToPath(new URL('./src/generated/shared/components', import.meta.url)),
        '/snippets/lib': fileURLToPath(new URL('./src/generated/shared/lib', import.meta.url)),
      },
    },
  },
});
