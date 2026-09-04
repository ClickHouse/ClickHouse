/**
 * Minimal React implementations of the Mintlify built-ins that the snippet
 * JSX components reference as implicit globals. SSR-safe, no hooks.
 */
import type { ReactNode, CSSProperties } from "react";

type Children = { children?: ReactNode };

export function Frame({ children, caption }: Children & { caption?: string }) {
  return (
    <figure className="ch-frame">
      <div className="ch-frame-shell">
        <div className="ch-frame-content">{children}</div>
      </div>
      {caption ? <figcaption>{caption}</figcaption> : null}
    </figure>
  );
}

export function Icon({ icon, size, color }: { icon?: string; size?: number | string; color?: string }) {
  const style: CSSProperties = { width: size ?? "1em", height: size ?? "1em", color };
  return <span className="ch-icon" data-icon={icon} style={style} aria-hidden="true" />;
}

export function Card({ title, href, children }: Children & { title?: string; href?: string; icon?: string }) {
  const body = (
    <>
      {title ? <div className="ch-card-title">{title}</div> : null}
      {children ? <div className="ch-card-body">{children}</div> : null}
    </>
  );
  return href ? (
    <a className="ch-card" href={href}>{body}</a>
  ) : (
    <div className="ch-card">{body}</div>
  );
}

export function Accordion({ title, children, defaultOpen }: Children & { title?: string; defaultOpen?: boolean }) {
  return (
    <details className="ch-accordion" open={defaultOpen}>
      <summary>{title}</summary>
      <div>{children}</div>
    </details>
  );
}

export const Expandable = Accordion;

export function Tooltip({ tip, children }: Children & { tip?: string }) {
  return (
    <span className="ch-tooltip" title={tip}>{children}</span>
  );
}

export function Banner({ children }: Children) {
  return <div className="ch-banner" role="note">{children}</div>;
}

export function Badge({ children, color }: Children & { color?: string }) {
  return <span className="ch-badge" data-color={color}>{children}</span>;
}
