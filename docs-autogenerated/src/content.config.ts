import { defineCollection } from 'astro:content';
import { glob } from 'astro/loaders';
import { z } from 'astro/zod';

const reference = defineCollection({
  loader: glob({
    base: './.mintlify/docs',
    pattern: '**/*.{md,mdx}',
    generateId: ({ entry }) => entry.replace(/\.mdx?$/, ''),
  }),
  schema: z.object({
    title: z.string(),
    sidebarTitle: z.string(),
    description: z.string(),
    keywords: z.array(z.string()),
    doc_type: z.literal('reference'),
    route: z.string(),
    stableId: z.string(),
    entityKind: z.string(),
    featureState: z.enum(['beta', 'experimental']).nullable(),
    sourcePath: z.string(),
    contentHash: z.string(),
    aliases: z.array(z.string()),
    legacyRoutes: z.array(z.string()),
  }),
});

export const collections = { reference };
