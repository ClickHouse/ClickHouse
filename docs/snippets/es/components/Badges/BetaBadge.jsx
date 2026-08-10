import { galaxyOnClick } from '/snippets/lib/galaxy.jsx';

export const BetaBadge = ({ link, galaxyTrack, galaxyEvent }) => {
    if (link) {
        return (
            <a
                href={link}
                target="_blank"
                rel="noopener noreferrer"
                className="betaBadge"
                onClick={galaxyTrack && galaxyEvent ? galaxyOnClick(galaxyEvent) : undefined}
            >
                <span>Beta</span>
            </a>
        )
    }

    return (
        <a
            href="/es/reference/settings/beta-and-experimental-features#beta-features"
            className="betaBadge"
        >
            <span>Funcionalidad Beta</span>
        </a>
    )
};