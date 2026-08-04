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
                <span>Бета</span>
            </a>
        )
    }

    return (
        <a
            href="/ru/reference/settings/beta-and-experimental-features#beta-features"
            className="betaBadge"
        >
            <span>Возможность в бета-версии</span>
        </a>
    )
};