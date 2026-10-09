const STATEMENT_ROUTE_PREFIX = '/docs/reference/statements';

function slug(value) {
  return value
    .toLocaleLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
}

function commonTokenCount(left, right) {
  let count = 0;
  while (count < left.length && count < right.length && left[count] === right[count]) {
    count += 1;
  }
  return count;
}

function statementNameParts(name, parent) {
  const [primitive, operation = ''] = name.split(/\s+\.\.\.\s+/, 2);
  const primitiveTokens = primitive.split(/\s+/);
  const parentTokens = parent ? parent.split(/\s+/) : [];
  const sharedTokens = commonTokenCount(primitiveTokens, parentTokens);
  return {
    primitive: primitiveTokens.slice(sharedTokens).join(' '),
    operation,
  };
}

function statementRouteSegments(registration, registrationByName, visiting = new Set()) {
  if (visiting.has(registration.name)) {
    throw new Error(`Statement parent cycle involving ${registration.name}`);
  }
  const nextVisiting = new Set(visiting).add(registration.name);
  let segments = [];
  if (registration.parent) {
    const parent = registrationByName.get(registration.parent);
    if (!parent) {
      throw new Error(
        `Statement ${registration.name} has unknown parent ${registration.parent}`,
      );
    }
    segments = statementRouteSegments(parent, registrationByName, nextVisiting);
  }

  const { primitive, operation } = statementNameParts(
    registration.name,
    registration.parent,
  );
  for (const part of [primitive, operation]) {
    if (!part) continue;
    const segment = slug(part);
    if (!segment) throw new Error(`Cannot derive a route for statement ${registration.name}`);
    segments.push(segment);
  }
  return segments;
}

function extractedSqlKeywords(registration) {
  const keywords = new Set();
  const normalizedName = registration.name
    .replace(/\s+\.\.\.\s+/g, ' ')
    .replace(/\s+modifier$/i, '')
    .replace(/\s+/g, ' ')
    .trim();
  if (normalizedName && normalizedName !== registration.name) keywords.add(normalizedName);

  const { primitive, operation } = statementNameParts(
    registration.name,
    registration.parent,
  );
  if (operation) {
    const primitiveName = [registration.parent, primitive].filter(Boolean).join(' ');
    if (primitiveName && primitiveName !== registration.name) keywords.add(primitiveName);
    keywords.add(operation);
  }
  if (/\s+modifier$/i.test(registration.name)) {
    keywords.add(registration.name.replace(/\s+modifier$/i, ''));
    keywords.add('modifier');
  }

  const terminalKeyword = normalizedName.split(' ').at(-1);
  if (terminalKeyword) {
    for (const match of registration.content.matchAll(/`([^`\n]+)`/g)) {
      const candidate = match[1].replace(/\s+/g, ' ').trim();
      if (
        candidate !== registration.name
        && candidate.length <= 64
        && /^[A-Z][A-Z0-9]*(?:[ _-][A-Z0-9]+){0,7}$/.test(candidate)
        && (
          candidate === terminalKeyword
          || candidate.endsWith(` ${terminalKeyword}`)
        )
      ) {
        keywords.add(candidate);
      }
    }
  }

  keywords.delete(registration.name);
  return [...keywords];
}

function statementSidebarTitle(registration) {
  if (/\s+modifier$/i.test(registration.name)) {
    return registration.name.replace(/\s+modifier$/i, '');
  }
  if (!registration.parent) return registration.name;

  const { primitive, operation } = statementNameParts(
    registration.name,
    registration.parent,
  );
  if (operation) return [primitive, '...', operation].filter(Boolean).join(' ');
  return primitive || registration.name;
}

function validateLegacyRoutes(definition, registrationByName) {
  if (
    definition.schemaVersion !== 1
    || !definition.routes
    || Array.isArray(definition.routes)
  ) {
    throw new Error('Unsupported legacy statement route definition');
  }
  for (const [name, routes] of Object.entries(definition.routes)) {
    if (!registrationByName.has(name)) {
      throw new Error(`Legacy routes reference unknown statement ${name}`);
    }
    if (
      !Array.isArray(routes)
      || routes.length === 0
      || routes.some((route) => typeof route !== 'string' || !route.startsWith('/docs/'))
      || new Set(routes).size !== routes.length
    ) {
      throw new Error(`Invalid legacy routes for statement ${name}`);
    }
  }
}

export function statementId(name) {
  const key = slug(name);
  if (!key) throw new Error(`Cannot derive an id for statement ${name}`);
  return `reference:statement:${key}`;
}

export function resolveStatementPages(registrations, legacyRouteDefinition) {
  const registrationByName = new Map();
  for (const registration of registrations) {
    if (registrationByName.has(registration.name)) {
      throw new Error(`Duplicate statement registration: ${registration.name}`);
    }
    registrationByName.set(registration.name, registration);
  }
  validateLegacyRoutes(legacyRouteDefinition, registrationByName);

  const parentNames = new Set(
    registrations.map(({ parent }) => parent).filter(Boolean),
  );
  const ids = new Set();
  const routes = new Set();
  const pages = registrations.map((registration) => {
    const id = statementId(registration.name);
    const route = `${STATEMENT_ROUTE_PREFIX}/${statementRouteSegments(
      registration,
      registrationByName,
    ).join('/')}`;
    if (ids.has(id)) throw new Error(`Duplicate generated statement id: ${id}`);
    if (routes.has(route)) throw new Error(`Duplicate generated statement route: ${route}`);
    ids.add(id);
    routes.add(route);

    const legacyRoutes = new Set([
      route.replace('/docs/reference/', '/docs/sql-reference/'),
      ...(legacyRouteDefinition.routes[registration.name] ?? []),
    ]);
    if (!registration.parent && parentNames.has(registration.name)) {
      legacyRoutes.add(`${route}/index`);
    }
    legacyRoutes.delete(route);

    return {
      id,
      name: statementSidebarTitle(registration),
      title: registration.name,
      route,
      legacyRoutes: [...legacyRoutes],
      keywords: extractedSqlKeywords(registration),
      sourcePath: registration.sourcePath,
      parent: registration.parent || null,
      related: registration.related,
      content: registration.content,
    };
  });

  return pages.sort((left, right) => left.title.localeCompare(right.title));
}

export { extractedSqlKeywords, statementRouteSegments, statementSidebarTitle };
